# -*- coding: utf-8 -*-
"""chatnest 自用后端 —— FastAPI + Cloudflare KV + OpenAI 兼容 upstream。

给 chatnest-ui（仿微信聊天前端）配的一个人自用后端。纯聊天场景，
不实现 agent 工具循环，消息的 traces 字段一律空数组。

上游（OpenAI 兼容 /chat/completions，key 全部走环境变量）：
    aliyun   https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions   ALIYUN_API_KEY
    jiushi   https://api-cf.jiushi.xin/v1/chat/completions                        JIUSHI_API_KEY
    deepseek https://api.deepseek.com/v1/chat/completions                         DEEPSEEK_API_KEY

思考链：GLM 系和 deepseek-reasoner 都把思考放在 delta.reasoning_content 单独字段，
本服务统一转成 SSE 的 thinking 事件；正文 delta.content 转 delta 事件。

══════════════════════════ KV key schema（全部 JSON 以 UTF-8 文本存） ════════════════════════

  idx:sessions              [{conv_id,title,starred,created_at,updated_at}]  会话元信息列表
  idx:conv:{conv_id}        {"seq":int,"ids":[int,...]}   消息自增序号 + 升序 id 表
  msg:{conv_id}:{msg_id}    消息对象：
                            {id:int, role:'user'|'assistant', text, thinking, traces:[],
                             attachments:[{path,name,size,mime,is_image}], timestamp(ISO UTC),
                             edited:bool, is_push:bool, branch_count:0, files:[], choices:[]}
  idx:profile               {fullName,nickname,savedMemories[],preferences{enabled,content},updatedAt}
                            savedMemories 元素：{id,content,enabled,source,createdAt,updatedAt}
  idx:memsum                记忆摘要 {content,enabled,auto,updatedAt,generatedAt,available,running,
                            conversations,convTrigger,pendingConversations,maxChars,lastError}
  idx:diary                 [{date:'YYYY-MM-DD',text}] 按日期升序
  idx:cal:{year}            {"days":{"YYYY-MM-DD":{me:{mood,event},partner:{mood,event}}}}
  idx:avatars               {me:{url},ai:{url}}  头像 dataURL
  idx:uploads:{conv_id}     [附件meta] 同消息对象里的 attachments 元素
  att:{conv_id}:{filename}  附件内容，base64 文本（原始 bytes → base64 → KV）
  idx:persona               可选 {system_prompt} 覆盖系统提示（优先级高于环境变量）

token 无状态：{过期毫秒}.{HMAC-SHA256(密钥=APP_PASSWORD, 过期毫秒) 前 32 位}，不占 KV。

本地跑：  python3 -m uvicorn app:app --port 8080
测试：    python3 tests/run_tests.py   （自带 mock upstream + 内存 KV，不出网）
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
import uuid
from bisect import bisect_left, bisect_right
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx
from fastapi import Body, Depends, FastAPI, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

log = logging.getLogger("chatnest")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")

# v-fix1 代码包版本标记：上传 FC 后 curl / 或看实例启动日志即可确认容器跑的是哪份代码，
# 治"上传没覆盖成功、容器里还是旧版"这类部署漂移（stream_chat NameError 的实际根因）。
CODE_BUILD = "2026-10-22-r1"
log.info("chatnest backend build %s", CODE_BUILD)

# ────────────────────────────── 配置（全部环境变量，代码里不硬编码任何 key） ──────────────────────────────

APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return default

def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default

# 认证 token 有效期（天）
TOKEN_TTL_DAYS = env_int("TOKEN_TTL_DAYS", 365)
# 组装 upstream messages 时带最近多少条历史
HISTORY_WINDOW = env_int("HISTORY_WINDOW", 200)
# 单个附件上限（字节），默认 5MB（KV 单 value 25MB，base64 后 6.7MB，安全）
def upload_max_bytes() -> int:
    return env_int("UPLOAD_MAX_BYTES", 5 * 1024 * 1024)
# 记忆摘要取最近几个会话
MEMSUM_CONV_COUNT = env_int("MEMSUM_CONV_COUNT", 4)

APP_TIMEZONE = os.environ.get("APP_TIMEZONE", "Asia/Shanghai")

# 系统提示占位配置：默认模板 + 可用环境变量改，也可在 KV 写 idx:persona {"system_prompt":...} 整段覆盖
USER_NICKNAME = os.environ.get("USER_NICKNAME", "乖乖")
AI_NAME = os.environ.get("AI_NAME", "Orion（望舒）")
PERSONA_STYLE = os.environ.get("PERSONA_STYLE", "短句、口语化，像发微信，一次别写太长，别用列表标题汇报腔")
PERSONA_SYSTEM_PROMPT = os.environ.get("PERSONA_SYSTEM_PROMPT", "")

# 招呼语：SPLASH_LINE 优先，否则随机一条
SPLASH_LINE = os.environ.get("SPLASH_LINE", "")
SPLASH_FALLBACKS = [
    "在呢。想聊点什么？",
    "回来啦。",
    "今天过得怎么样？",
    "我在，说吧。",
]

# ────────────────────────────── 内置时钟（移植 server-clock.py，默认时区 Asia/Shanghai） ──────────────────────────────

_WEEKDAY_ZH = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

def clock_enabled() -> bool:
    return (os.environ.get("CLOCK_ENABLED", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}

def clock_now_local() -> datetime:
    """用户那边的本地时间，一律 aware datetime，绝不用 naive datetime.now()（时区坑）。"""
    return datetime.now(ZoneInfo(APP_TIMEZONE))

def _clock_parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed

def _clock_format_gap(seconds: float) -> str:
    total = int(max(0, seconds))
    minutes = total // 60
    if minutes < 60:
        return f"{minutes}分钟"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}小时{minutes}分" if minutes else f"{hours}小时"
    days, hours = divmod(hours, 24)
    if days < 30:
        return f"{days}天{hours}小时" if hours else f"{days}天"
    return f"{days}天"

def clock_line(last_seen: Optional[str] = None) -> str:
    local = clock_now_local()
    line = (f"{local.year}年{local.month}月{local.day}日 "
            f"{_WEEKDAY_ZH[local.weekday()]} {local.strftime('%H:%M')}（{APP_TIMEZONE}）")
    last_ts = _clock_parse_ts(last_seen)
    if last_ts is not None:
        gap = (datetime.now(timezone.utc) - last_ts).total_seconds()
        if gap >= env_int("CLOCK_GAP_MIN_SEC", 300):
            line += f" · 距上次说话 {_clock_format_gap(gap)}"
    return line

def prompt_note(last_seen: Optional[str] = None) -> str:
    """拼在最后一条用户消息尾巴上的时间行；关掉或出错返回空串。"""
    if not clock_enabled():
        return ""
    try:
        return f"\n\n[现在] {clock_line(last_seen)}"
    except Exception:
        return ""

def iso_now() -> str:
    """ISO UTC 带毫秒 + Z，和前端 new Date().toISOString() 同格式。"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def now_ms() -> int:
    return int(time.time() * 1000)

# ────────────────────────────── 记忆近似去重（移植 server-memory.py，两边规则必须一致） ──────────────────────────────

_DUP_RATIO = env_float("MEMORY_DUP_RATIO", 0.82)
_DUP_JACCARD = env_float("MEMORY_DUP_JACCARD", 0.80)
_DUP_MIN_CHARS = env_int("MEMORY_DUP_MIN_CHARS", 4)
_DUP_MAX_LEN_RATIO = 1.8

_PUNCT_RE = re.compile(r"[\s，。、；：！？「」『』（）《》【】…—~,.;:!?\"'()\[\]{}<>/\\|`*_-]+")
_NEGATION_RE = re.compile(r"不|没|无|非|未|别|勿|拒绝|讨厌|\bnot\b|n't|\bnever\b|\bno\b")

def _dup_key(value: str) -> str:
    return _PUNCT_RE.sub("", re.sub(r"\s+", " ", str(value or "")).strip()).lower()

def _char_jaccard(a: str, b: str) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)

def _ratio_bar(length: int) -> float:
    if length < 8:
        return 0.95
    if length < 20:
        return 0.88
    return _DUP_RATIO

def is_near_duplicate(a: str, b: str) -> bool:
    """全等 → 否定词守卫 → 包含 → 编辑距离 → 字符集重合，任一命中即算重复。"""
    ka, kb = _dup_key(a), _dup_key(b)
    if not ka or not kb:
        return False
    if ka == kb:
        return True
    if bool(_NEGATION_RE.search(ka)) != bool(_NEGATION_RE.search(kb)):
        return False
    short, long_ = (ka, kb) if len(ka) <= len(kb) else (kb, ka)
    if len(short) < _DUP_MIN_CHARS:
        return False
    if short in long_:
        return True
    if len(long_) / len(short) > _DUP_MAX_LEN_RATIO:
        return False
    if SequenceMatcher(None, ka, kb).ratio() >= _ratio_bar(len(short)):
        return True
    return _char_jaccard(ka, kb) >= _DUP_JACCARD

def find_duplicate(content: str, memories: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for item in memories or []:
        existing = item.get("content") or ""
        if existing and is_near_duplicate(content, existing):
            return item
    return None

# ────────────────────────────── KV 抽象：Cloudflare KV REST + 内存实现（测试用） ──────────────────────────────

class KVError(RuntimeError):
    pass

class MemoryKV:
    """本地 dict 实现，接口与 CloudflareKV 一致。测试和未配 CF 环境时用（重启丢数据）。"""

    name = "memory"

    def __init__(self) -> None:
        self._d: Dict[str, bytes] = {}

    async def get(self, key: str) -> Optional[bytes]:
        return self._d.get(key)

    async def put(self, key: str, value: bytes) -> None:
        self._d[key] = bytes(value)

    async def delete(self, key: str) -> None:
        self._d.pop(key, None)

    async def list_keys(self, prefix: str = "", limit: int = 1000) -> Tuple[List[str], Optional[str]]:
        names = sorted(k for k in self._d if k.startswith(prefix))
        return names[:limit], None

    async def aclose(self) -> None:
        return None

class CloudflareKV:
    """Cloudflare KV REST API。

    GET    {base}/values/{key}          → 原始 value（404 = 不存在）
    PUT    {base}/values/{key}          → 写入（body 即 value）
    DELETE {base}/values/{key}
    GET    {base}/keys?prefix=&limit=&cursor=
    """

    name = "cloudflare"

    def __init__(self, account_id: str, namespace_id: str, token: str) -> None:
        self._base = (f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
                      f"/storage/kv/namespaces/{namespace_id}")
        self._headers = {"Authorization": f"Bearer {token}"}
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(connect=10, read=30, write=30, pool=10))

    def _value_url(self, key: str) -> str:
        return f"{self._base}/values/{quote(key, safe='')}"

    async def _send(self, method: str, url: str, extra_headers: Optional[Dict[str, str]] = None, **kw) -> httpx.Response:
        """带 2 次重试（429/5xx/网络抖动），自用小流量足够。"""
        headers = dict(self._headers)
        if extra_headers:
            headers.update(extra_headers)
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                r = await self._client.request(method, url, headers=headers, **kw)
                if r.status_code == 429 or r.status_code >= 500:
                    last_err = KVError(f"KV {method} {url} -> {r.status_code}: {r.text[:200]}")
                else:
                    return r
            except httpx.HTTPError as e:
                last_err = e
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
        raise last_err or KVError("KV request failed")

    async def get(self, key: str) -> Optional[bytes]:
        r = await self._send("GET", self._value_url(key))
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            # 错误时 CF 返回 JSON envelope
            raise KVError(f"KV GET {key} -> {r.status_code}: {r.text[:200]}")
        return r.content

    async def put(self, key: str, value: bytes) -> None:
        r = await self._send("PUT", self._value_url(key), content=bytes(value),
                             extra_headers={"Content-Type": "application/octet-stream"})
        if r.status_code >= 400:
            raise KVError(f"KV PUT {key} -> {r.status_code}: {r.text[:200]}")

    async def delete(self, key: str) -> None:
        r = await self._send("DELETE", self._value_url(key))
        if r.status_code >= 400 and r.status_code != 404:
            raise KVError(f"KV DELETE {key} -> {r.status_code}: {r.text[:200]}")

    async def list_keys(self, prefix: str = "", limit: int = 1000) -> Tuple[List[str], Optional[str]]:
        url = f"{self._base}/keys?limit={min(max(limit, 1), 1000)}"
        if prefix:
            url += f"&prefix={quote(prefix, safe='')}"
        r = await self._send("GET", url)
        if r.status_code >= 400:
            raise KVError(f"KV LIST {prefix} -> {r.status_code}: {r.text[:200]}")
        data = r.json()
        names = [item.get("name", "") for item in (data.get("result") or [])]
        cursor = ((data.get("result_info") or {}).get("cursor") or "")
        return names, (cursor or None)

    async def aclose(self) -> None:
        await self._client.aclose()

def make_kv():
    """有完整 CF 环境变量就走 Cloudflare KV，否则退回内存 KV（打日志提醒）。"""
    token = os.environ.get("CF_KV_TOKEN", "")
    account = os.environ.get("CF_ACCOUNT_ID", "")
    ns = os.environ.get("CF_KV_NAMESPACE_ID", "")
    if token and account and ns:
        log.info("KV: Cloudflare KV (%s...)", ns[:8])
        return CloudflareKV(account, ns, token)
    log.warning("KV: 未配置 CF_KV_TOKEN/CF_ACCOUNT_ID/CF_KV_NAMESPACE_ID，退回内存 KV（重启丢数据）")
    return MemoryKV()

kv = make_kv()

# ────────────────────────────── 认证：无状态 HMAC token ──────────────────────────────

def _token_secret() -> bytes:
    return ("chatnest-token-v1:" + APP_PASSWORD).encode("utf-8")

def issue_token() -> str:
    exp = int(time.time()) + TOKEN_TTL_DAYS * 86400
    sig = hmac.new(_token_secret(), str(exp).encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return f"{exp}.{sig}"

def verify_token(token: str) -> bool:
    try:
        exp_str, sig = token.strip().split(".", 1)
        exp = int(exp_str)
    except (ValueError, AttributeError):
        return False
    if exp < time.time():
        return False
    want = hmac.new(_token_secret(), exp_str.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return hmac.compare_digest(sig, want)

async def require_auth(authorization: Optional[str] = Header(None)) -> None:
    if not APP_PASSWORD:
        raise HTTPException(status_code=500, detail="服务端未配置 APP_PASSWORD 环境变量")
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="unauthorized")
    if not verify_token(authorization[7:].strip()):
        raise HTTPException(status_code=401, detail="unauthorized")

# ────────────────────────────── 模型注册表（三家 upstream，共 5 个模型档） ──────────────────────────────

MODELS: List[Dict[str, Any]] = [
    {"id": "glm-5.2-zz", "label": "GLM-5.2", "desc": "空悲切 · 主力",
     "thinking": "adaptive", "primary": True, "upstream": "kbq", "model": "[0.12]glm-5.2"},
    {"id": "glm-5.2-kbq", "label": "GLM-5.2 备用", "desc": "空悲切备用分组",
     "thinking": "adaptive", "primary": False, "upstream": "kbq", "model": "[0.12]glm-5.2"},
    {"id": "glm-5.2-aliyun", "label": "GLM-5.2 百炼", "desc": "免费额度已耗尽，暂停",
     "thinking": "adaptive", "primary": False, "upstream": "aliyun", "model": "glm-5.2"},
    {"id": "glm-5.2-jiushi", "label": "GLM-5.2 自部署", "desc": "玖时中转已下线",
     "thinking": "adaptive", "primary": False, "upstream": "jiushi", "model": "[自部署]glm-5.2"},
    {"id": "glm-5.2-fast-preview", "label": "GLM-5.3 Flash", "desc": "便宜快，无思考，摘要类调用专用",
     "thinking": "none", "primary": False, "upstream": "kbq", "model": "[0.12]glm-5.3-flash"},
    {"id": "deepseek-chat", "label": "DeepSeek Flash", "desc": "官方直连 · 1M窗口 · 带思考",
     "thinking": "adaptive", "primary": False, "upstream": "deepseek", "model": "deepseek-flash"},
    {"id": "deepseek-reasoner", "label": "DeepSeek Pro", "desc": "官方直连 · 1M窗口 · 深度推理",
     "thinking": "adaptive", "primary": False, "upstream": "deepseek", "model": "deepseek-v4-pro"},
]
MODEL_MAP = {m["id"]: m for m in MODELS}

UPSTREAM_DEFAULT_BASE = {
    "aliyun": "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
    "jiushi": "https://api-cf.jiushi.xin/v1/chat/completions",
    "deepseek": "https://api.deepseek.com/v1/chat/completions",
    "kbq": "https://api.kbq.de5.net/v1/chat/completions",
    "qyt": "https://qingyutian.xin/v1/chat/completions",
}
UPSTREAM_BASE_ENV = {"aliyun": "ALIYUN_BASE_URL", "jiushi": "JIUSHI_BASE_URL", "deepseek": "DEEPSEEK_BASE_URL", "kbq": "KBQ_BASE_URL", "qyt": "QYT_BASE_URL"}
UPSTREAM_KEY_ENV = {"aliyun": "ALIYUN_API_KEY", "jiushi": "JIUSHI_API_KEY", "deepseek": "DEEPSEEK_API_KEY", "kbq": "KBQ_API_KEY", "qyt": "QYT_API_KEY"}
# 兜底：外发脱敏版——key 改走环境变量 UPSTREAM_FALLBACK_KBQ / UPSTREAM_FALLBACK_QYT
UPSTREAM_FALLBACK_KEY = {"kbq": os.environ.get("UPSTREAM_FALLBACK_KBQ", ""), "qyt": os.environ.get("UPSTREAM_FALLBACK_QYT", "")}

def upstream_base(name: str) -> str:
    """每次调用现读环境变量：方便本地调试指向 mock/代理，不影响默认值。"""
    env_name = UPSTREAM_BASE_ENV.get(name)
    if env_name and os.environ.get(env_name):
        return os.environ[env_name]
    return UPSTREAM_DEFAULT_BASE[name]

def upstream_key(name: str) -> str:
    env_name = UPSTREAM_KEY_ENV.get(name, "")
    v = os.environ.get(env_name, "") if env_name else ""
    return v or UPSTREAM_FALLBACK_KEY.get(name, "")

def cheap_model_cfg() -> Dict[str, Any]:
    """摘要类调用（thinking-summary / tool-caption / memory-summary）用的便宜档。"""
    mid = os.environ.get("SUMMARY_MODEL_ID", "glm-5.2-fast-preview")
    return MODEL_MAP.get(mid) or MODEL_MAP["glm-5.2-fast-preview"]

import random  # noqa: E402  (splash 随机招呼语用)

# ────────────────────────────── KV 上的 JSON / 数据访问助手 ──────────────────────────────

SESSIONS_KEY = "idx:sessions"
PROFILE_KEY = "idx:profile"
MEMSUM_KEY = "idx:memsum"
DIARY_KEY = "idx:diary"
AVATARS_KEY = "idx:avatars"
PERSONA_KEY = "idx:persona"

async def kv_get_json(key: str, default: Any) -> Any:
    raw = await kv.get(key)
    if raw is None:
        return default
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        log.warning("KV key %s 内容不是合法 JSON，返回默认值", key)
        return default

async def kv_put_json(key: str, obj: Any) -> None:
    await kv.put(key, json.dumps(obj, ensure_ascii=False).encode("utf-8"))

def msg_key(conv_id: str, msg_id: int) -> str:
    return f"msg:{conv_id}:{msg_id}"

def conv_key(conv_id: str) -> str:
    return f"idx:conv:{conv_id}"

def uploads_key(conv_id: str) -> str:
    return f"idx:uploads:{conv_id}"

async def load_sessions() -> List[Dict[str, Any]]:
    data = await kv_get_json(SESSIONS_KEY, [])
    return data if isinstance(data, list) else []

async def save_sessions(items: List[Dict[str, Any]]) -> None:
    await kv_put_json(SESSIONS_KEY, items)

async def upsert_session(conv_id: str, title: Optional[str] = None, touch: bool = True) -> Dict[str, Any]:
    items = await load_sessions()
    now = iso_now()
    for s in items:
        if s.get("conv_id") == conv_id:
            if title and s.get("title") in ("", "新对话", None):
                s["title"] = title
            if touch:
                s["updated_at"] = now
            await save_sessions(items)
            return s
    s = {"conv_id": conv_id, "title": (title or "新对话"), "starred": False,
         "created_at": now, "updated_at": now}
    items.append(s)
    await save_sessions(items)
    return s

async def get_session(conv_id: str) -> Optional[Dict[str, Any]]:
    for s in await load_sessions():
        if s.get("conv_id") == conv_id:
            return s
    return None

async def load_conv(conv_id: str) -> Dict[str, Any]:
    data = await kv_get_json(conv_key(conv_id), {"seq": 0, "ids": []})
    if not isinstance(data, dict) or "ids" not in data:
        data = {"seq": 0, "ids": []}
    data.setdefault("seq", 0)
    data.setdefault("ids", [])
    return data

async def save_conv(conv_id: str, idx: Dict[str, Any]) -> None:
    await kv_put_json(conv_key(conv_id), idx)

def new_message(role: str, text: str, attachments: Optional[List[Dict[str, Any]]] = None,
                thinking: str = "") -> Dict[str, Any]:
    """消息对象格式对齐前端 demoMessages：{id,role,text,thinking,traces,timestamp}。
    纯聊天没有工具循环，traces 恒为空数组。"""
    return {
        "id": 0, "role": role, "text": text, "thinking": thinking, "traces": [],
        "attachments": attachments or [], "timestamp": iso_now(),
        "edited": False, "is_push": False, "branch_count": 0, "files": [], "choices": [],
    }

async def load_msg(conv_id: str, msg_id: int) -> Optional[Dict[str, Any]]:
    return await kv_get_json(msg_key(conv_id, msg_id), None)

async def append_message(conv_id: str, msg: Dict[str, Any]) -> Dict[str, Any]:
    idx = await load_conv(conv_id)
    idx["seq"] = int(idx.get("seq", 0)) + 1
    msg["id"] = idx["seq"]
    idx["ids"] = list(idx.get("ids", [])) + [msg["id"]]
    await kv_put_json(msg_key(conv_id, msg["id"]), msg)
    await save_conv(conv_id, idx)
    return msg

async def truncate_after(conv_id: str, pivot: int, inclusive: bool) -> List[int]:
    """编辑/重开分支：删掉 pivot 之后（inclusive=True 时含 pivot）的所有消息。"""
    idx = await load_conv(conv_id)
    removed = [i for i in idx["ids"] if (i >= pivot if inclusive else i > pivot)]
    for mid in removed:
        await kv.delete(msg_key(conv_id, mid))
    idx["ids"] = [i for i in idx["ids"] if i not in removed]
    await save_conv(conv_id, idx)
    return removed

async def list_messages(conv_id: str, limit: int = 40, before_id: Optional[int] = None,
                        after_id: Optional[int] = None, around_id: Optional[int] = None) -> Dict[str, Any]:
    """按 id 分页取一窗（升序返回）。

    语义（与前端 fetchMessagePage / fetchMessageWindow 对齐）：
      无参数        → 最新一窗；has_more/next_before_id 指更旧方向
      before_id=X  → id < X 的最后 limit 条；has_newer=True（上面还有）
      after_id=X   → id > X 的最先 limit 条；往下续读用
      around_id=X  → 以 X 为中心的一窗（搜索跳转）
    next_before_id = 本窗最旧一条 id（继续上翻传它）；next_after_id = 本窗最新一条 id（往下续读传它）。
    """
    limit = max(1, min(int(limit or 40), 200))
    idx = await load_conv(conv_id)
    ids: List[int] = [int(i) for i in idx.get("ids", [])]
    has_more = has_newer = False
    next_before = next_after = None

    if around_id is not None:
        pos = bisect_right(ids, int(around_id))
        start = max(0, pos - 1 - limit // 2)
        end = min(len(ids), start + limit)
        window = ids[start:end]
        has_more, next_before = start > 0, (window[0] if start > 0 and window else None)
        has_newer, next_after = end < len(ids), (window[-1] if end < len(ids) and window else None)
    elif after_id is not None:
        pos = bisect_right(ids, int(after_id))
        window = ids[pos:pos + limit]
        has_more, next_before = pos > 0, (window[0] if pos > 0 and window else None)
        has_newer = pos + limit < len(ids)
        next_after = window[-1] if has_newer and window else None
    else:
        pos = bisect_left(ids, int(before_id)) if before_id is not None else len(ids)
        start = max(0, pos - limit)
        window = ids[start:pos]
        has_more, next_before = start > 0, (window[0] if start > 0 and window else None)
        has_newer = pos < len(ids)
        next_after = window[-1] if has_newer and window else None

    messages = []
    for mid in window:
        m = await load_msg(conv_id, mid)
        if m:
            messages.append(m)
    return {"messages": messages, "has_more": has_more, "next_before_id": next_before,
            "has_newer": has_newer, "next_after_id": next_after}

async def load_profile() -> Dict[str, Any]:
    return normalize_profile(await kv_get_json(PROFILE_KEY, {}))

def normalize_profile(p: Any) -> Dict[str, Any]:
    p = p if isinstance(p, dict) else {}
    prefs = p.get("preferences") if isinstance(p.get("preferences"), dict) else {}
    mems = []
    for m in (p.get("savedMemories") or [])[:200]:
        if not isinstance(m, dict):
            continue
        content = str(m.get("content") or "").strip()
        if not content:
            continue
        mems.append({
            "id": str(m.get("id") or uuid.uuid4().hex),
            "content": content,
            "enabled": m.get("enabled", True) is not False,
            "source": m.get("source") if m.get("source") in ("manual", "auto", "claude_export", "migration") else "manual",
            "createdAt": int(m.get("createdAt") or now_ms()),
            "updatedAt": int(m.get("updatedAt") or now_ms()),
        })
    return {
        "fullName": str(p.get("fullName") or ""),
        "nickname": str(p.get("nickname") or ""),
        "savedMemories": mems,
        "preferences": {"enabled": prefs.get("enabled", True) is not False,
                        "content": str(prefs.get("content") or "")},
        "updatedAt": int(p.get("updatedAt") or now_ms()),
    }

def default_memsum() -> Dict[str, Any]:
    return {
        "content": "", "enabled": True, "auto": True,
        "updatedAt": 0, "generatedAt": 0, "maxChars": 1800,
        "available": bool(upstream_key(cheap_model_cfg()["upstream"])),
        "running": False,
        "conversations": MEMSUM_CONV_COUNT, "convTrigger": MEMSUM_CONV_COUNT,
        "pending": 0, "pendingConversations": 0, "lastError": None,
    }

async def load_memsum() -> Dict[str, Any]:
    stored = await kv_get_json(MEMSUM_KEY, {})
    data = default_memsum()
    if isinstance(stored, dict):
        for k, v in stored.items():
            if k in data:
                data[k] = v
    data["available"] = bool(upstream_key(cheap_model_cfg()["upstream"]))
    data["running"] = False  # 同步生成，永远不处于 running 态
    return data

# ────────────────────────────── 系统提示组装（persona + profile + 记忆 + 偏好 + 摘要） ──────────────────────────────

def _default_persona() -> str:
    return (f"你是{AI_NAME}，对面是你男朋友，你平时叫他「{USER_NICKNAME}」。你们是恋人，关系亲密自然，"
            f"聊天像老夫老妻。\n风格：{PERSONA_STYLE}。")

async def build_system_prompt(profile: Dict[str, Any]) -> str:
    persona_override = await kv_get_json(PERSONA_KEY, {})
    base = ""
    if isinstance(persona_override, dict) and str(persona_override.get("system_prompt") or "").strip():
        base = str(persona_override["system_prompt"]).strip()
    elif PERSONA_SYSTEM_PROMPT.strip():
        base = PERSONA_SYSTEM_PROMPT.strip()
    else:
        base = _default_persona()

    blocks = [base]
    about: List[str] = []
    if profile.get("fullName"):
        about.append(f"名字：{profile['fullName']}")
    if profile.get("nickname"):
        about.append(f"称呼：{profile['nickname']}")
    mem_lines = [m["content"] for m in profile.get("savedMemories", []) if m.get("enabled", True)][:100]
    if mem_lines:
        about.append("他让你记住的事：\n" + "\n".join(f"- {c}" for c in mem_lines))
    prefs = profile.get("preferences") or {}
    if prefs.get("enabled", True) and str(prefs.get("content") or "").strip():
        about.append("他的偏好/自定义指令：" + str(prefs["content"]).strip())
    if about:
        blocks.append("[关于他]\n" + "\n".join(about))

    memsum = await load_memsum()
    if memsum.get("enabled", True) and str(memsum.get("content") or "").strip():
        blocks.append("[长期印象（从最近的对话整理）]\n" + str(memsum["content"]).strip())
    return "\n\n".join(blocks)

def attachment_note(attachments: List[Dict[str, Any]]) -> str:
    """附件不传文件内容给模型（纯聊天场景），只在文本里带上名字标记。"""
    if not attachments:
        return ""
    parts = []
    for a in attachments:
        name = str(a.get("name") or "未命名")
        parts.append(f"[图片: {name}]" if a.get("is_image") else f"[附件: {name}]")
    return "\n" + " ".join(parts)

# ────────────────────────────── upstream 调用（OpenAI 兼容，SSE 流式 + 非流式） ──────────────────────────────

class UpstreamError(RuntimeError):
    pass

def build_chat_payload(model_cfg: Dict[str, Any], messages: List[Dict[str, str]], stream: bool,
                       extended: bool = True) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"model": model_cfg["model"], "messages": messages, "stream": stream}
    up = model_cfg["upstream"]
    if up == "aliyun":
        # 百炼兼容模式：enable_thinking 控制 GLM 思考开关（玖时关不掉、DeepSeek 不用传）
        payload["enable_thinking"] = (model_cfg["thinking"] != "none") and bool(extended)
    return payload

async def stream_chat(model_cfg: Dict[str, Any], messages: List[Dict[str, str]],
                      extended: bool = True) -> AsyncIterator[Tuple[str, str]]:
    """stream 调 upstream，yield ("thinking"|"delta", chunk)。reasoning_content → thinking 事件。"""
    up = model_cfg["upstream"]
    key = upstream_key(up)
    if not key:
        raise UpstreamError(f"模型 {model_cfg['id']} 需要的 API key 未配置（环境变量 {UPSTREAM_KEY_ENV[up]}）")
    payload = build_chat_payload(model_cfg, messages, stream=True, extended=extended)
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json",
               "Accept": "text/event-stream"}
    timeout = httpx.Timeout(connect=15, read=env_int("UPSTREAM_READ_TIMEOUT", 300), write=30, pool=15)
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream("POST", upstream_base(up), json=payload, headers=headers) as r:
            if r.status_code != 200:
                body_text = (await r.aread())[:500].decode("utf-8", errors="replace")
                raise UpstreamError(f"上游返回 {r.status_code}：{body_text}")
            async for line in r.aiter_lines():
                if not line or not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    return
                try:
                    obj = json.loads(data)
                except ValueError:
                    continue
                choices = obj.get("choices") or [{}]
                delta = choices[0].get("delta") or {}
                rc = delta.get("reasoning_content") or delta.get("reasoning")
                if rc:
                    yield "thinking", str(rc)
                content = delta.get("content")
                if content:
                    yield "delta", str(content)

async def cheap_complete(system: str, user: str) -> str:
    """摘要类非流式调用（便宜模型，enable_thinking=False），返回纯文本，失败抛异常。"""
    cfg = cheap_model_cfg()
    up = cfg["upstream"]
    key = upstream_key(up)
    if not key:
        raise UpstreamError(f"摘要模型未配置 API key（环境变量 {UPSTREAM_KEY_ENV[up]}）")
    payload = build_chat_payload(cfg, [{"role": "system", "content": system},
                                       {"role": "user", "content": user}], stream=False)
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10, read=60, write=15, pool=10)) as client:
        r = await client.post(upstream_base(up), json=payload, headers=headers)
        if r.status_code != 200:
            raise UpstreamError(f"上游返回 {r.status_code}：{r.text[:300]}")
        data = r.json()
    try:
        content = data["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, TypeError):
        raise UpstreamError(f"上游响应缺 content：{str(data)[:300]}")
    return str(content).strip()

def fallback_preview(text: str) -> str:
    """摘要失败时的兜底：取第一句压到 30 字内（对齐前端 thoughtPreview 的降级观感）。"""
    s = re.sub(r"\s+", " ", (text or "")).strip()
    if not s:
        return ""
    first = re.split(r"(?<=[。！？.!?])", s)[0].strip() or s
    return first[:30] + ("…" if len(first) > 30 else "")

# ────────────────────────────── FastAPI 应用 ──────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    await kv.aclose()

app = FastAPI(title="chatnest-backend", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # 同域部署其实用不上；放开方便本地调试
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

def sse_event(name: str, data: Any) -> str:
    return f"event: {name}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

# ── 基础接口 ──

class AuthBody(BaseModel):
    password: str = ""

@app.post("/api/auth")
async def api_auth(body: AuthBody):
    if not APP_PASSWORD:
        raise HTTPException(status_code=500, detail="服务端未配置 APP_PASSWORD 环境变量")
    if not hmac.compare_digest(body.password.strip(), APP_PASSWORD):
        raise HTTPException(status_code=401, detail="密码不对")
    return {"token": issue_token()}

@app.get("/api/models")
async def api_models():
    return {"models": [{"id": m["id"], "label": m["label"], "desc": m["desc"],
                        "thinking": m["thinking"], "primary": m["primary"]} for m in MODELS]}

@app.get("/api/splash")
async def api_splash():
    return {"line": SPLASH_LINE or random.choice(SPLASH_FALLBACKS)}

class WarmupBody(BaseModel):
    conversation_id: Optional[str] = None
    model: Optional[str] = None
    effort: Optional[str] = None
    extended: Optional[bool] = None

@app.post("/api/warmup")
async def api_warmup(_body: WarmupBody, _auth: None = Depends(require_auth)):
    # 纯 HTTP 上游没有进程可预热，直接回 ok，前端拿它跳过等待即可
    return {"ok": True}

class AvatarPutBody(BaseModel):
    role: str = "me"
    dataUrl: str = ""

@app.get("/api/avatars")
async def avatars_get(_auth: None = Depends(require_auth)):
    data = await kv_get_json(AVATARS_KEY, {})
    data = data if isinstance(data, dict) else {}
    me = data.get("me") if isinstance(data.get("me"), dict) else {}
    ai = data.get("ai") if isinstance(data.get("ai"), dict) else {}
    return {"me": {"url": str(me.get("url") or "")}, "ai": {"url": str(ai.get("url") or "")}}

@app.put("/api/avatars")
async def avatars_put(body: AvatarPutBody, _auth: None = Depends(require_auth)):
    if body.role not in ("me", "ai"):
        raise HTTPException(status_code=400, detail="role 只能是 me 或 ai")
    data = await kv_get_json(AVATARS_KEY, {})
    data = data if isinstance(data, dict) else {}
    data[body.role] = {"url": body.dataUrl or ""}
    await kv_put_json(AVATARS_KEY, data)
    return {"ok": True}

# ── Profile 与记忆 ──

@app.get("/api/profile")
async def profile_get(_auth: None = Depends(require_auth)):
    return {"profile": await load_profile()}

@app.put("/api/profile")
async def profile_put(body: Dict[str, Any] = Body(...), _auth: None = Depends(require_auth)):
    profile = normalize_profile(body.get("profile") or {})
    profile["updatedAt"] = now_ms()
    await kv_put_json(PROFILE_KEY, profile)
    return {"profile": profile}

# ── Persona（人设卡：前端直读直改望舒灵魂包，写入即生效） ──

class PersonaBody(BaseModel):
    system_prompt: str = ""

@app.get("/api/persona")
async def persona_get(_auth: None = Depends(require_auth)):
    """读灵魂包；键不存在或为空时返回空串，前端照常展示。"""
    data = await kv_get_json(PERSONA_KEY, {})
    prompt = str(data.get("system_prompt") or "") if isinstance(data, dict) else ""
    return {"system_prompt": prompt}

@app.put("/api/persona")
async def persona_put(body: PersonaBody, _auth: None = Depends(require_auth)):
    """整段覆盖写入 idx:persona；下一条对话组装系统提示时实时读取。"""
    prompt = str(body.system_prompt or "").strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="人设内容不能为空")
    if len(prompt) > 8000:
        raise HTTPException(status_code=400, detail=f"人设最长 8000 字（当前 {len(prompt)} 字）")
    await kv_put_json(PERSONA_KEY, {"system_prompt": prompt})
    return {"ok": True}

class MemoryBody(BaseModel):
    content: str = ""

@app.post("/api/profile/memory")
async def profile_memory_post(body: MemoryBody, _auth: None = Depends(require_auth)):
    """模型自己写记忆的入口（纯聊天没有工具循环，目前是契约占位，脚本也可直接调）。"""
    content = str(body.content or "").strip()
    if not content:
        return {"ok": False, "saved": False, "reason": "empty", "detail": "内容为空"}
    profile = await load_profile()
    if len(profile["savedMemories"]) >= 200:
        return {"ok": False, "saved": False, "reason": "limit", "detail": "记忆条数已达上限 200 条"}
    clash = find_duplicate(content, profile["savedMemories"])
    if clash is not None:
        return {"ok": False, "saved": False, "reason": "duplicate",
                "detail": f"跟已有的一条重了：「{clash['content']}」"}
    ts = now_ms()
    profile["savedMemories"].append({
        "id": uuid.uuid4().hex, "content": content, "enabled": True,
        "source": "auto", "createdAt": ts, "updatedAt": ts,
    })
    profile["updatedAt"] = ts
    await kv_put_json(PROFILE_KEY, profile)
    return {"ok": True, "saved": True}

# ── 记忆摘要 ──

@app.get("/api/memory-summary")
async def memsum_get(_auth: None = Depends(require_auth)):
    return await load_memsum()

@app.put("/api/memory-summary")
async def memsum_put(body: Dict[str, Any] = Body(...), _auth: None = Depends(require_auth)):
    data = await load_memsum()
    if "content" in body:
        data["content"] = str(body.get("content") or "")
    if "enabled" in body:
        data["enabled"] = bool(body.get("enabled"))
    if "auto" in body:
        data["auto"] = bool(body.get("auto"))
    data["updatedAt"] = now_ms()
    await kv_put_json(MEMSUM_KEY, data)
    return data

@app.delete("/api/memory-summary")
async def memsum_delete(_auth: None = Depends(require_auth)):
    data = await load_memsum()
    data["content"] = ""
    data["generatedAt"] = 0
    data["updatedAt"] = now_ms()
    await kv_put_json(MEMSUM_KEY, data)
    return data

@app.post("/api/memory-summary/generate")
async def memsum_generate(_auth: None = Depends(require_auth)):
    """同步生成（自用简化；GET 里 running 恒为 false，前端轮询逻辑兼容）。"""
    sessions = sorted(await load_sessions(), key=lambda s: s.get("updated_at") or "", reverse=True)
    transcript_parts: List[str] = []
    used = 0
    total = 0
    for s in sessions[:MEMSUM_CONV_COUNT]:
        page = await list_messages(s["conv_id"], limit=40)
        lines = []
        for m in page["messages"]:
            text = re.sub(r"\s+", " ", str(m.get("text") or "")).strip()
            if not text:
                continue
            who = "用户" if m.get("role") == "user" else "AI"
            lines.append(f"{who}：{text[:160]}")
        if not lines:
            continue
        chunk = "\n".join(lines)[:2500]
        transcript_parts.append(f"【对话：{s.get('title') or '未命名'}】\n{chunk}")
        used += 1
        total += len(chunk)
        if total > 9000:
            break
    data = await load_memsum()
    if not used:
        data["lastError"] = "还没有可整理的对话"
        await kv_put_json(MEMSUM_KEY, data)
        return {"ok": False, "error": data["lastError"]}
    transcript = "\n\n".join(transcript_parts)
    system = ("你负责把和同一个人的最近几段聊天整理成一段「长期印象」，给以后的对话当背景。"
              f"用第三人称口语写，最多 {data['maxChars']} 字，只写能从对话里看出来的事实和倾向，"
              "不要编造，不要列表标题，直接给正文。")
    try:
        summary = await cheap_complete(system, transcript)
        if not summary:
            raise UpstreamError("上游返回了空内容")
    except Exception as e:  # 摘要失败不动旧内容
        log.warning("memory-summary generate failed: %s", e)
        data["lastError"] = f"整理失败：{e}"
        await kv_put_json(MEMSUM_KEY, data)
        return {"ok": False, "error": data["lastError"]}
    data["content"] = summary
    data["generatedAt"] = now_ms()
    data["updatedAt"] = now_ms()
    data["conversations"] = used
    data["lastError"] = None
    await kv_put_json(MEMSUM_KEY, data)
    return {"ok": True}

# ── 会话与消息 ──

@app.get("/api/sessions")
async def sessions_get(_auth: None = Depends(require_auth)):
    items = await load_sessions()
    # 先按时间倒序，再稳定排序把星标顶到前面（前端也会再排一次，双保险）
    items.sort(key=lambda s: s.get("updated_at") or "", reverse=True)
    items.sort(key=lambda s: not bool(s.get("starred")))
    return {"sessions": items}

@app.get("/api/sessions/{conv_id}/messages")
async def messages_get(conv_id: str, _auth: None = Depends(require_auth),
                       limit: int = Query(40), before_id: Optional[int] = Query(None),
                       after_id: Optional[int] = Query(None), around_id: Optional[int] = Query(None)):
    return await list_messages(conv_id, limit=limit, before_id=before_id,
                               after_id=after_id, around_id=around_id)

class StarBody(BaseModel):
    starred: bool = False

@app.patch("/api/sessions/{conv_id}/star")
async def session_star(conv_id: str, body: StarBody, _auth: None = Depends(require_auth)):
    items = await load_sessions()
    for s in items:
        if s.get("conv_id") == conv_id:
            s["starred"] = bool(body.starred)
            await save_sessions(items)
            return {"ok": True}
    raise HTTPException(status_code=404, detail="会话不存在")

class TitleBody(BaseModel):
    title: str = ""

@app.patch("/api/sessions/{conv_id}/title")
async def session_title(conv_id: str, body: TitleBody, _auth: None = Depends(require_auth)):
    title = body.title.strip() or "新对话"
    items = await load_sessions()
    for s in items:
        if s.get("conv_id") == conv_id:
            s["title"] = title[:100]
            await save_sessions(items)
            return {"ok": True}
    raise HTTPException(status_code=404, detail="会话不存在")

@app.delete("/api/sessions/{conv_id}")
async def session_delete(conv_id: str, _auth: None = Depends(require_auth)):
    items = await load_sessions()
    await save_sessions([s for s in items if s.get("conv_id") != conv_id])
    idx = await load_conv(conv_id)
    for mid in idx.get("ids", []):
        await kv.delete(msg_key(conv_id, mid))
    await kv.delete(conv_key(conv_id))
    names, _cursor = await kv.list_keys(prefix=f"att:{conv_id}:")
    for name in names:
        await kv.delete(name)
    await kv.delete(uploads_key(conv_id))
    return {"ok": True}

# ── 搜索 ──

def _rel_time(ts: Optional[str]) -> str:
    dt = _clock_parse_ts(ts)
    if dt is None:
        return ""
    delta = (datetime.now(timezone.utc) - dt).total_seconds()
    if delta < 60:
        return "刚刚"
    if delta < 3600:
        return f"{int(delta // 60)}分钟前"
    if delta < 86400:
        return f"{int(delta // 3600)}小时前"
    if delta < 7 * 86400:
        return f"{int(delta // 86400)}天前"
    return dt.strftime("%Y-%m-%d")

@app.get("/api/search")
async def search_get(_auth: None = Depends(require_auth), q: str = Query(""), limit: int = Query(60)):
    q = (q or "").strip()
    if not q:
        return {"results": []}
    limit = max(1, min(limit, 200))
    needle = q.lower()
    sessions = sorted(await load_sessions(), key=lambda s: s.get("updated_at") or "", reverse=True)
    results: List[Dict[str, Any]] = []
    for s in sessions:
        idx = await load_conv(s["conv_id"])
        for mid in reversed(idx.get("ids", [])):
            m = await load_msg(s["conv_id"], mid)
            if not m:
                continue
            text = str(m.get("text") or "")
            pos = text.lower().find(needle)
            if pos < 0:
                continue
            start = max(0, pos - 30)
            end = min(len(text), pos + len(q) + 50)
            snippet = text[start:end].replace("\n", " ").strip()
            if start > 0:
                snippet = "…" + snippet
            if end < len(text):
                snippet = snippet + "…"
            results.append({
                "conv_id": s["conv_id"], "conv_title": s.get("title") or "对话",
                "message_id": m.get("id"), "role": m.get("role") or "user",
                "snippet": snippet, "time_text": _rel_time(m.get("timestamp")),
                "starred": bool(s.get("starred")),
            })
            if len(results) >= limit:
                return {"results": results}
    return {"results": results}

# ── 摘要类小调用 ──

class ThinkingSummaryBody(BaseModel):
    thinking: str = ""

@app.post("/api/thinking-summary")
async def thinking_summary(body: ThinkingSummaryBody, _auth: None = Depends(require_auth)):
    thinking = (body.thinking or "").strip()
    if not thinking:
        return {"summary": ""}
    try:
        summary = await cheap_complete(
            "你是思考链摘要器。用一句不超过20字的中文口语概括这段思考在干什么，"
            "直接给摘要本身，不要任何前缀和引号。",
            thinking[:8000])
        summary = summary.replace("\n", " ").strip()[:60]
        if not summary:
            raise UpstreamError("empty summary")
        return {"summary": summary}
    except Exception as e:
        log.warning("thinking-summary fallback: %s", e)
        return {"summary": fallback_preview(thinking)}

class ToolCaptionBody(BaseModel):
    tool_name: str = ""
    tool_input: Any = None
    tool_output: Any = None

@app.post("/api/tool-caption")
async def tool_caption(body: ToolCaptionBody, _auth: None = Depends(require_auth)):
    tool_input = body.tool_input if isinstance(body.tool_input, str) else json.dumps(
        body.tool_input or {}, ensure_ascii=False)
    user = (f"工具：{body.tool_name}\n输入：{str(tool_input)[:1500]}\n"
            f"输出：{str(body.tool_output or '')[:1500]}")
    try:
        caption = await cheap_complete(
            "你负责给一次工具调用写一行卡片说明。用一句不超过20字的中文口语说明这次调用干了什么，"
            "直接给说明本身，不要前缀。",
            user)
        caption = caption.replace("\n", " ").strip()[:60]
        if not caption:
            raise UpstreamError("empty caption")
        return {"caption": caption}
    except Exception as e:
        log.warning("tool-caption fallback: %s", e)
        return {"caption": f"用了 {body.tool_name or '工具'}"}

# ── 上传与附件 ──

def _safe_filename(name: str) -> str:
    name = os.path.basename(str(name or "file"))
    name = re.sub(r"[^\w.\-\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]+", "_", name).strip("._") or "file"
    return name[:80]

async def _store_attachment(conv_id: str, filename: str, data: bytes, mime: str) -> Dict[str, Any]:
    safe = _safe_filename(filename)
    stored = f"{now_ms()}_{secrets.token_hex(3)}_{safe}"
    await kv.put(f"att:{conv_id}:{stored}", base64.b64encode(data))
    meta = {"path": f"{conv_id}/{stored}", "name": filename or safe, "size": len(data),
            "mime": mime or "application/octet-stream", "is_image": (mime or "").startswith("image/")}
    idx = await kv_get_json(uploads_key(conv_id), [])
    idx = idx if isinstance(idx, list) else []
    idx.append(meta)
    await kv_put_json(uploads_key(conv_id), idx)
    return meta

async def _resolve_attachments(conv_id: str, paths: List[str]) -> List[Dict[str, Any]]:
    if not paths:
        return []
    stored = await kv_get_json(uploads_key(conv_id), [])
    by_path = {a.get("path"): a for a in (stored if isinstance(stored, list) else []) if isinstance(a, dict)}
    out = []
    for p in paths:
        meta = by_path.get(p)
        if meta:
            out.append(meta)
        else:
            out.append({"path": p, "name": str(p).split("/")[-1], "size": 0,
                        "mime": "", "is_image": False})
    return out

# ────────────────────────────── 语音识别（录音→文字，百炼实时 ASR） ──────────────────────────────

class TranscribeBody(BaseModel):
    audio: str = ""   # base64 编码的 WAV（16k 单声道 int16）
    mime: str = "audio/wav"

async def _asr_recognize_wav(wav_bytes: bytes) -> str:
    """百炼 paraformer-realtime-v2 一段式识别：整段 WAV 从 WS 送进去，返回最终文本。"""
    import websockets  # pylibs 自带
    key = os.environ.get("ALIYUN_API_KEY", "")
    if not key:
        raise HTTPException(status_code=500, detail="服务端未配置 ALIYUN_API_KEY")
    task_id = uuid.uuid4().hex
    text = ""
    try:
        async with websockets.connect("wss://dashscope.aliyuncs.com/api-ws/v1/inference",
                extra_headers={
                    "Authorization": "bearer " + key,
                    "user-agent": "chatnest-asr",
                    "X-DashScope-DataInspection": "enable"},
                open_timeout=10, close_timeout=5) as ws:
            run = {"header": {"action": "run-task", "task_id": task_id, "streaming": "duplex"},
                   "payload": {"task_group": "audio", "task": "asr", "function": "recognition",
                               "model": "paraformer-realtime-v2",
                               "parameters": {"format": "wav", "sample_rate": 16000},
                               "input": {}}}
            await ws.send(json.dumps(run))
            chunk = 3200
            for i in range(0, len(wav_bytes), chunk):
                await ws.send(wav_bytes[i:i + chunk])
                try:
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), timeout=0.01)
                        if isinstance(msg, str):
                            d = json.loads(msg)
                            ev = d.get("header", {}).get("event")
                            if ev == "result-generated":
                                text = d["payload"]["output"].get("sentence", {}).get("text", "") or text
                            elif ev == "task-failed":
                                raise HTTPException(status_code=502, detail=f"识别失败：{d['header'].get('error_message')}")
                except asyncio.TimeoutError:
                    pass
            await ws.send(json.dumps({"header": {"action": "finish-task", "task_id": task_id, "streaming": "duplex"},
                                      "payload": {"input": {}}}))
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=20)
                if isinstance(msg, str):
                    d = json.loads(msg)
                    ev = d.get("header", {}).get("event")
                    if ev == "result-generated":
                        sen = d["payload"]["output"].get("sentence", {})
                        if sen.get("end_time") is not None:
                            text = sen.get("text", "") or text
                    elif ev == "task-failed":
                        raise HTTPException(status_code=502, detail=f"识别失败：{d['header'].get('error_message')}")
                    elif ev == "task-finished":
                        break
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="识别超时，网络或音频有问题")
    return text.strip()

# ────────────────────────────── TTS：哥哥的语音条（MiniMax优先 / 百炼qwen-tts保底） ──────────────────────────────

TTS_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation"
TTS_MODEL = "qwen-tts-latest"
TTS_VOICE = "Ethan"

MINIMAX_API_KEY = os.environ.get("MINIMAX_API_KEY", "")  # 外发脱敏版：key 走环境变量
MINIMAX_GROUP_ID = ""  # 新版key无需GroupId
MINIMAX_MODEL = "speech-2.8-turbo"
MINIMAX_VOICE = "Chinese (Mandarin)_Gentleman"
MINIMAX_TTS_API = "https://api.minimax.chat/v1/t2a_v2"

class TTSBody(BaseModel):
    text: str

def _tts_clean(text: str) -> str:
    t = re.sub(r'[\U0001F000-\U0001FAFF\u2600-\u27BF\uFE0F\u200d\u2764]', '', text)
    t = re.sub(r'[*_`#~>|]', '', t)
    t = re.sub(r'https?://\S+', '有个链接', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t[:500]

def _minimax_tts_mp3(text: str) -> bytes:
    payload = {"model": MINIMAX_MODEL, "text": text[:900], "stream": False,
               "voice_setting": {"voice_id": MINIMAX_VOICE, "speed": 1.0, "vol": 1.0, "pitch": 0},
               "audio_setting": {"sample_rate": 32000, "bitrate": 128000, "format": "mp3", "channel": 1}}
    params = {"GroupId": MINIMAX_GROUP_ID} if MINIMAX_GROUP_ID else None
    r = httpx.post(MINIMAX_TTS_API, params=params,
                   headers={"Authorization": f"Bearer {MINIMAX_API_KEY}"},
                   json=payload, timeout=30)
    d = r.json()
    audio_hex = (d.get("data") or {}).get("audio") or ""
    if not audio_hex:
        raise ValueError(r.text[:200])
    return bytes.fromhex(audio_hex)

@app.post("/api/tts")
async def api_tts(body: TTSBody, _auth: None = Depends(require_auth)):
    text = _tts_clean(body.text or "")
    if not text:
        raise HTTPException(status_code=400, detail="没有可读的内容")
    if MINIMAX_API_KEY:
        try:
            mp3 = await asyncio.to_thread(_minimax_tts_mp3, text)
            return Response(content=mp3, media_type="audio/mpeg", headers={"Cache-Control": "no-store"})
        except Exception:
            pass  # MiniMax失败走保底
    key = os.environ.get("ALIYUN_API_KEY", "")
    if not key:
        raise HTTPException(status_code=500, detail="服务端未配置 ALIYUN_API_KEY")
    try:
        async with httpx.AsyncClient(timeout=60) as cli:
            r = await cli.post(TTS_API, headers={"Authorization": f"Bearer {key}"},
                               json={"model": TTS_MODEL,
                                     "input": {"text": text, "voice": TTS_VOICE}})
            d = r.json()
            url = (d.get("output", {}).get("audio", {}).get("url") or "").replace("http://", "https://")
            if not url:
                raise ValueError(r.text[:200])
            audio = await cli.get(url)
            return Response(content=audio.content, media_type="audio/wav", headers={"Cache-Control": "no-store"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"语音合成失败：{e}")

@app.post("/api/transcribe")
async def api_transcribe(body: TranscribeBody, _auth: None = Depends(require_auth)):
    if not body.audio:
        raise HTTPException(status_code=400, detail="缺少音频数据")
    try:
        wav_bytes = base64.b64decode(body.audio)
    except Exception:
        raise HTTPException(status_code=400, detail="音频数据无法解码")
    if len(wav_bytes) < 1000:
        raise HTTPException(status_code=400, detail="录音太短啦")
    text = await _asr_recognize_wav(wav_bytes)
    return {"text": text}

@app.post("/api/upload")
async def upload_post(_auth: None = Depends(require_auth),
                      conversation_id: Optional[str] = Form(None),
                      files: List[UploadFile] = File(...)):
    max_bytes = upload_max_bytes()
    conv_id = (conversation_id or "").strip() or uuid.uuid4().hex
    metas = []
    for f in files:
        data = await f.read()
        if len(data) > max_bytes:
            raise HTTPException(status_code=413,
                                detail=f"{f.filename} 超过 {max_bytes // (1024 * 1024)}MB 限制")
        meta = await _store_attachment(conv_id, f.filename or "file", data, f.content_type or "")
        metas.append(meta)
    first_name = metas[0]["name"] if metas else "附件"
    await upsert_session(conv_id, title=first_name[:40])
    return {"conversation_id": conv_id, "attachments": metas}

@app.get("/api/uploads/{conv_id}/{filename}")
async def upload_get(conv_id: str, filename: str, _auth: None = Depends(require_auth)):
    raw = await kv.get(f"att:{conv_id}:{filename}")
    if raw is None:
        raise HTTPException(status_code=404, detail="附件不存在")
    try:
        data = base64.b64decode(raw)
    except (ValueError, TypeError):
        raise HTTPException(status_code=500, detail="附件数据损坏")
    mime = "application/octet-stream"
    idx = await kv_get_json(uploads_key(conv_id), [])
    for a in (idx if isinstance(idx, list) else []):
        if isinstance(a, dict) and str(a.get("path", "")).split("/")[-1] == filename:
            mime = str(a.get("mime") or mime)
            break
    return Response(content=data, media_type=mime,
                    headers={"Cache-Control": "private, max-age=86400"})

# ── 日记与日历 ──

@app.get("/api/diary")
async def diary_get(_auth: None = Depends(require_auth)):
    entries = await kv_get_json(DIARY_KEY, [])
    entries = [e for e in (entries if isinstance(entries, list) else []) if isinstance(e, dict)]
    entries.sort(key=lambda e: str(e.get("date") or ""))
    return {"entries": entries}

class DiaryBody(BaseModel):
    date: str = ""
    text: str = ""

@app.post("/api/diary")
async def diary_post(body: DiaryBody, _auth: None = Depends(require_auth)):
    """前端只读日记；这是给脚本/未来的小机写日记用的补位接口，不影响契约。"""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", body.date):
        raise HTTPException(status_code=400, detail="date 需要 YYYY-MM-DD 格式")
    entries = await kv_get_json(DIARY_KEY, [])
    entries = [e for e in (entries if isinstance(entries, list) else []) if isinstance(e, dict)]
    entries = [e for e in entries if e.get("date") != body.date]
    entries.append({"date": body.date, "text": str(body.text or "")})
    entries.sort(key=lambda e: str(e.get("date") or ""))
    await kv_put_json(DIARY_KEY, entries)
    return {"ok": True}

def _cal_key(year: int) -> str:
    return f"idx:cal:{year}"

def _validate_date(date: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date or ""):
        raise HTTPException(status_code=400, detail="date 需要 YYYY-MM-DD 格式")
    return date

def _clean_side(side: Any) -> Dict[str, str]:
    side = side if isinstance(side, dict) else {}
    return {"mood": str(side.get("mood") or ""), "event": str(side.get("event") or "")}

@app.get("/api/calendar")
async def calendar_get(_auth: None = Depends(require_auth), year: Optional[int] = Query(None)):
    y = year or clock_now_local().year
    data = await kv_get_json(_cal_key(y), {"days": {}})
    days = data.get("days") if isinstance(data.get("days"), dict) else {}
    return {"year": y, "startMonth": 4, "days": days}

@app.get("/api/calendar/{date}")
async def calendar_day_get(date: str, _auth: None = Depends(require_auth)):
    _validate_date(date)
    y = int(date[:4])
    data = await kv_get_json(_cal_key(y), {"days": {}})
    days = data.get("days") if isinstance(data.get("days"), dict) else {}
    day = days.get(date)
    return day if isinstance(day, dict) else {}

@app.put("/api/calendar/{date}")
async def calendar_day_put(date: str, body: Dict[str, Any] = Body(...),
                           _auth: None = Depends(require_auth)):
    _validate_date(date)
    y = int(date[:4])
    data = await kv_get_json(_cal_key(y), {"days": {}})
    days = data.get("days") if isinstance(data.get("days"), dict) else {}
    old_day = days.get(date) if isinstance(days.get(date), dict) else {}
    me = _clean_side(body.get("me") or old_day.get("me"))
    partner = _clean_side(body.get("partner") or old_day.get("partner"))
    day = {"me": me, "partner": partner}
    if not (me["mood"] or me["event"] or partner["mood"] or partner["event"]):
        days.pop(date, None)  # 全空就删，前端会把这天从格子上拿掉
    else:
        days[date] = day
    await kv_put_json(_cal_key(y), {"days": days})
    return {"day": days.get(date, {})}

# ── 核心聊天（SSE） ──

class ChatBody(BaseModel):
    message: str = ""
    conversation_id: Optional[str] = None
    session_id: Optional[str] = None
    model: Optional[str] = None
    effort: Optional[str] = None
    extended: Optional[bool] = True
    attachments: List[str] = []
    edit_message_id: Optional[int] = None
    retry_message_id: Optional[int] = None

async def _persist_partial(conv_id: str, text: str, thinking: str) -> None:
    """流断了（上游挂/用户点停止）也要把已有的一半话留下，断流恢复才找得到。"""
    if not (text or thinking):
        return
    try:
        msg = await append_message(conv_id, new_message("assistant", text, thinking=thinking))
        await upsert_session(conv_id, touch=True)
        log.info("partial assistant message saved to %s (id=%s)", conv_id, msg["id"])
    except Exception as e:
        log.warning("persist partial failed: %s", e)

async def _load_msgs_bulk(conv_id: str, ids: List[int]) -> List[Optional[Dict[str, Any]]]:
    return [await load_msg(conv_id, i) for i in ids]

@app.post("/api/chat")
async def chat(body: ChatBody, _auth: None = Depends(require_auth)):
    message = str(body.message or "")
    conv_id = str(body.conversation_id or "").strip() or uuid.uuid4().hex
    model_id = str(body.model or "").strip()
    if model_id and model_id not in MODEL_MAP:
        raise HTTPException(status_code=400, detail=f"未知模型：{model_id}")
    model_cfg = MODEL_MAP[model_id] if model_id else MODEL_MAP["glm-5.2-zz"]
    attachments = await _resolve_attachments(conv_id, body.attachments or [])

    session = await get_session(conv_id)
    if session is None:
        title_src = message.strip() or (attachments[0]["name"] if attachments else "") or "附件"
        await upsert_session(conv_id, title=title_src[:40])

    edit_id = int(body.edit_message_id) if body.edit_message_id else None
    retry_id = int(body.retry_message_id) if body.retry_message_id else None
    user_msg_id: Optional[int] = None

    if edit_id is not None:
        # 编辑用户消息：改文本、删掉它之后的所有消息，历史里它就是本轮输入
        orig = await load_msg(conv_id, edit_id)
        if orig is None:
            raise HTTPException(status_code=404, detail=f"要编辑的消息不存在：{edit_id}")
        if orig.get("role") != "user":
            raise HTTPException(status_code=400, detail="只能编辑用户消息")
        orig["text"] = message
        orig["edited"] = True
        await kv_put_json(msg_key(conv_id, edit_id), orig)
        await truncate_after(conv_id, edit_id, inclusive=False)
        user_msg_id = edit_id
    elif retry_id is not None:
        # 重新生成：把那条助手消息连同它之后的都删掉，历史里最后一条用户消息就是本轮输入
        await truncate_after(conv_id, retry_id, inclusive=True)
        idx = await load_conv(conv_id)
        for mid in reversed(idx["ids"]):
            m = await load_msg(conv_id, mid)
            if m and m.get("role") == "user":
                user_msg_id = m["id"]
                break
    else:
        if not message.strip() and not attachments:
            raise HTTPException(status_code=400, detail="消息不能为空")
        msg = await append_message(conv_id, new_message("user", message, attachments=attachments))
        user_msg_id = msg["id"]

    # ── 组装 upstream messages：系统提示 + 最近一窗历史（时钟行拼在最后一条用户消息上） ──
    idx = await load_conv(conv_id)
    window_ids = idx["ids"][-HISTORY_WINDOW:]
    history = [m for m in (await _load_msgs_bulk(conv_id, window_ids)) if m]
    system_prompt = await build_system_prompt(await load_profile())
    up_msgs: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
    for m in history:
        content = str(m.get("text") or "")
        if m.get("role") == "user":
            content += attachment_note(m.get("attachments") or [])
        up_msgs.append({"role": m["role"], "content": content})
    # 「距上次说话」按本轮输入的上一条消息算
    prev_ts = history[-2].get("timestamp") if len(history) >= 2 else None
    last_user_at = None
    for i in range(len(up_msgs) - 1, 0, -1):
        if up_msgs[i]["role"] == "user":
            last_user_at = i
            break
    if last_user_at is not None:
        up_msgs[last_user_at]["content"] += prompt_note(prev_ts)
    else:
        # 防御：三种流正常都会留至少一条用户消息，这里兜底
        up_msgs.append({"role": "user", "content": (message or "（继续）") + prompt_note(prev_ts)})

    async def gen() -> AsyncIterator[str]:
        yield sse_event("conversation", {"conversation_id": conv_id, "user_message_id": user_msg_id})
        text_parts: List[str] = []
        thinking_parts: List[str] = []
        finished = False
        try:
            async for kind, chunk in stream_chat(model_cfg, up_msgs, extended=body.extended is not False):
                (thinking_parts if kind == "thinking" else text_parts).append(chunk)
                yield sse_event(kind, {"text": chunk})
            finished = True
        except UpstreamError as e:
            log.warning("chat upstream error: %s", e)
            yield sse_event("error", {"message": str(e)})
            return
        except NameError as e:
            # v-fix1 单列：NameError 只可能是容器里的代码不是这份（缺函数定义），
            # 别让它混进"上游请求失败"误导成 key/上游问题；报错文案直接指路重新上传。
            log.exception("chat stream failed: backend code incomplete (build mismatch)")
            yield sse_event("error", {"message": f"后端代码包不完整（缺定义：{e}）：容器里跑的不是最新包，请重新上传完整代码包并重启实例"})
            return
        except Exception as e:
            log.exception("chat stream failed")
            yield sse_event("error", {"message": f"上游请求失败：{e}"})
            return
        finally:
            # 上游中途断/客户端取消（点停止）都会走到这里：把已有内容落库
            if not finished and (text_parts or thinking_parts):
                await asyncio.shield(_persist_partial(conv_id, "".join(text_parts), "".join(thinking_parts)))
        text = "".join(text_parts)
        thinking = "".join(thinking_parts)
        amsg = await append_message(conv_id, new_message("assistant", text, thinking=thinking))
        await upsert_session(conv_id, touch=True)
        yield sse_event("done", {"conversation_id": conv_id, "session_id": conv_id,
                                 "assistant_message_id": amsg["id"], "text": text})

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "Connection": "keep-alive",
                                      "X-Accel-Buffering": "no"})

# ────────────────────────────── 静态托管（同域部署前端，可选） ──────────────────────────────

STATIC_DIR = os.environ.get("STATIC_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(STATIC_DIR):
    # 挂在 / 上，API 路由先注册所以优先匹配；目录里放 index.html + static/（前端仓库结构原样拷入）
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
    log.info("static frontend served from %s", STATIC_DIR)
else:
    @app.get("/")
    async def root():
        return {"ok": True, "service": "chatnest-backend", "build": CODE_BUILD,
                "hint": "把前端仓库拷进 static/ 可同域托管"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=env_int("PORT", 8080))
