# -*- coding: utf-8 -*-
"""MCP 模块端到端冒烟：mock MCP(http/sse) + mock upstream + 内存 KV，不出外网。
用法：把 fc-mcp-20261022-r2.diff 打到原始 app.py 上，然后
      pip3 install fastapi uvicorn httpx python-multipart tzdata
      python3 smoke_mcp.py
末尾输出 SMOKE OK 即全绿。"""
import asyncio, json, os, sys

os.environ["APP_PASSWORD"] = "test-pw"
os.environ["KBQ_BASE_URL"] = "http://127.0.0.1:8931/up"
os.environ["KBQ_API_KEY"] = "mock-key"
os.environ["MCP_CONNECT_TIMEOUT"] = "5"

sys.path.insert(0, "/tmp/fcapp")
import httpx, uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import app as backend

# ── mock upstream：首轮吐 tool_calls 分片（测累积），回灌后吐正文 ──
def _sse(delta=None, finish=None):
    d = {} if delta is None else delta
    ch = {"delta": d}
    if finish:
        ch["finish_reason"] = finish
    return "data: " + json.dumps({"choices": [ch]}, ensure_ascii=False) + "\n\n"

upstream = FastAPI()

@upstream.post("/up")
async def mock_up(req: Request):
    body = await req.json()
    has_tool_result = any(m.get("role") == "tool" for m in body["messages"])
    assert body.get("tools"), "第一轮请求必须带 tools"
    fname = body["tools"][0]["function"]["name"]   # 动态取全名，sanitize 规则变了也不挂

    async def gen():
        if not has_tool_result:
            yield _sse({"reasoning_content": "想一下…"})
            yield _sse({"tool_calls": [{"index": 0, "id": "call_1",
                                        "function": {"name": fname, "arguments": ""}}]})
            yield _sse({"tool_calls": [{"index": 0, "function": {"arguments": "{\"text\":"}}]})
            yield _sse({"tool_calls": [{"index": 0, "function": {"arguments": "\"你好\"}"}}]})
            yield _sse(finish="tool_calls")
        else:
            tool_msg = [m for m in body["messages"] if m.get("role") == "tool"][0]
            assert "ECHO:你好" in tool_msg["content"], "工具结果没回灌：" + tool_msg["content"]
            assert tool_msg["tool_call_id"] == "call_1"
            yield _sse({"content": "工具说："})
            yield _sse({"content": "你好"})
            yield _sse(finish="stop")
        yield "data: [DONE]\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")

# ── mock MCP server：streamable HTTP ──
mcp_http = FastAPI()

@mcp_http.post("/mcp")
async def mcp_ep(req: Request):
    body = await req.json()
    rid = body.get("id")
    m = body["method"]
    if m == "initialize":
        r = JSONResponse({"jsonrpc": "2.0", "id": rid,
                          "result": {"protocolVersion": "2025-03-26", "capabilities": {},
                                     "serverInfo": {"name": "mock"}}})
        r.headers["Mcp-Session-Id"] = "sid-123"
        return r
    if m == "notifications/initialized":
        return JSONResponse({}, status_code=202)
    if m == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": [
            {"name": "echo", "description": "回声测试",
             "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}},
            {"name": "add", "description": "加法",
             "inputSchema": {"type": "object", "properties": {"a": {"type": "number"}, "b": {"type": "number"}}}}]}}
    if m == "tools/call":
        name = body["params"]["name"]
        args = body["params"].get("arguments") or {}
        if name == "echo":
            return {"jsonrpc": "2.0", "id": rid, "result": {"content": [{"type": "text", "text": "ECHO:" + str(args.get("text", ""))}], "isError": False}}
        if name == "add":
            return {"jsonrpc": "2.0", "id": rid, "result": {"content": [{"type": "text", "text": "SUM:%s" % (args.get("a", 0) + args.get("b", 0))}], "isError": False}}
        return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32602, "message": "no such tool"}}
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "method not found"}}

# ── mock MCP server：legacy HTTP+SSE ──
mcp_sse = FastAPI()
SSE_Q = {}

@mcp_sse.get("/sse")
async def sse_stream(req: Request):
    sid = id(req)
    SSE_Q[sid] = asyncio.Queue()

    async def gen():
        yield "event: endpoint\ndata: /msg?sid=%s\n\n" % sid
        while True:
            data = await SSE_Q[sid].get()
            yield "event: message\ndata: " + data + "\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")

@mcp_sse.post("/msg")
async def sse_msg(req: Request):
    body = await req.json()
    q = SSE_Q[int(req.query_params.get("sid"))]
    rid = body.get("id")
    m = body["method"]
    if m == "initialize":
        res = {"protocolVersion": "2024-11-05", "capabilities": {}, "serverInfo": {"name": "mock-sse"}}
    elif m == "tools/list":
        res = {"tools": [{"name": "ping", "description": "ping", "inputSchema": {"type": "object", "properties": {}}}]}
    elif m == "tools/call":
        res = {"content": [{"type": "text", "text": "PONG"}], "isError": False}
    else:
        res = None
    await q.put(json.dumps({"jsonrpc": "2.0", "id": rid, "result": res}))
    return JSONResponse({}, status_code=202)

# ── 解析 SSE 回包（对齐前端解析） ──
def parse_sse(text):
    out = []
    for block in text.split("\n\n"):
        ev, data = "message", ""
        for line in block.split("\n"):
            if line.startswith("event:"):
                ev = line[6:].strip()
            elif line.startswith("data:"):
                data += line[5:].strip()
        if data:
            try:
                out.append((ev, json.loads(data)))
            except ValueError:
                pass
    return out

async def main():
    servers = [("up", upstream, 8931), ("mcp", mcp_http, 8932), ("sse", mcp_sse, 8933)]
    tasks = []
    for name, appx, port in servers:
        cfg = uvicorn.Config(appx, host="127.0.0.1", port=port, log_level="error")
        s = uvicorn.Server(cfg)
        tasks.append(asyncio.create_task(s.serve()))
        while not s.started:
            await asyncio.sleep(0.02)
    await asyncio.sleep(0.2)

    hdr = {"Authorization": "Bearer " + backend.issue_token()}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=backend.app), base_url="http://main", timeout=30) as c:
        # 认证
        r = await c.get("/api/mcp/servers")
        assert r.status_code == 401, r.status_code
        # 空列表
        r = await c.get("/api/mcp/servers", headers=hdr)
        assert r.json() == {"servers": []}, r.text
        # 新建 http 服务器（中文名测 sanitize）
        r = await c.post("/api/mcp/servers", headers=hdr,
                         json={"name": "测试服务器", "url": "http://127.0.0.1:8932/mcp", "type": "http"})
        assert r.status_code == 200, r.text
        sid1 = r.json()["server"]["id"]
        # check → 拉工具
        r = await c.post(f"/api/mcp/servers/{sid1}/check", headers=hdr, json={})
        srv = r.json()["server"]
        assert srv["connected"] is True, srv
        assert srv["totalTools"] == 2 and srv["toolCount"] == 2, srv
        assert [t["name"] for t in srv["tools"]] == ["echo", "add"], srv
        # 停用 add
        r = await c.post(f"/api/mcp/servers/{sid1}/tools/toggle", headers=hdr, json={"name": "add", "enabled": False})
        assert r.json()["server"]["toolCount"] == 1, r.text
        # 编辑（带 id 更新，不重复建）
        r = await c.post("/api/mcp/servers", headers=hdr,
                         json={"id": sid1, "name": "测试服务器2", "url": "http://127.0.0.1:8932/mcp", "type": "http", "enabled": True})
        servers_now = (await c.get("/api/mcp/servers", headers=hdr)).json()["servers"]
        assert len(servers_now) == 1 and servers_now[0]["name"] == "测试服务器2", servers_now
        # 聊天全链路：tool 事件 + 回灌 + 正文
        r = await c.post("/api/chat", headers=hdr, json={"message": "帮我echo一下你好"})
        assert r.status_code == 200
        evs = parse_sse(r.text)
        kinds = [e for e, _ in evs]
        assert "conversation" in kinds and "error" not in kinds, evs
        tool_ev = [o for e, o in evs if e == "tool"]
        assert tool_ev and tool_ev[0]["ok"] is True and tool_ev[0]["name"].startswith("mcp__") \
            and tool_ev[0]["name"].endswith("__echo"), tool_ev
        text = "".join(o["text"] for e, o in evs if e == "delta")
        assert text == "工具说：你好", text
        done = [o for e, o in evs if e == "done"][0]
        assert done["text"] == "工具说：你好"
        # 历史里 assistant 消息已落库
        aid = done["assistant_message_id"]
        r = await c.get(f"/api/sessions/{done['conversation_id']}/messages", headers=hdr)
        msgs = r.json().get("messages") or r.json()
        am = [m for m in msgs if m["id"] == aid][0]
        assert am["text"] == "工具说：你好", am
        # 新建 sse 服务器并直连测传输层
        r = await c.post("/api/mcp/servers", headers=hdr,
                         json={"name": "sse-srv", "url": "http://127.0.0.1:8933/sse", "type": "sse"})
        sid2 = r.json()["server"]["id"]
        r = await c.post(f"/api/mcp/servers/{sid2}/check", headers=hdr, json={})
        srv2 = r.json()["server"]
        assert srv2["connected"] is True and srv2["totalTools"] == 1, srv2
        raw = [x for x in await backend.mcp_load_servers() if x["id"] == sid2][0]
        async with backend.McpClient(raw) as cli:
            ok, out = await cli.call_tool("ping", {})
            assert ok and out == "PONG", (ok, out)
        # checkall
        r = await c.post("/api/mcp/servers/checkall", headers=hdr, json={})
        assert all(s["connected"] for s in r.json()["servers"]), r.text
        # 删除
        r = await c.delete(f"/api/mcp/servers/{sid2}", headers=hdr)
        assert r.json() == {"ok": True}
        assert len((await c.get("/api/mcp/servers", headers=hdr)).json()["servers"]) == 1
        # 工具结果截断保护 & 非法参数
        ok, out = await backend.mcp_execute("mcp__srv__echo", "不是json{", {})
        assert ok is False
        ok, out = await backend.mcp_execute("mcp__nope__x", "{}", {})
        assert ok is False and "未找到工具" in out
        # health
        r = await c.get("/")
        assert r.json().get("build") == backend.CODE_BUILD

    for t in tasks:
        t.cancel()
    print("SMOKE OK")

asyncio.run(main())
