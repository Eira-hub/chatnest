# chatnest UI 双人并行协作规范

> 本文件是唯一的协作约定来源。两个 agent 动手前先读这一页。
> 有冲突以本文件为准；要改本文件，改完在文末 CHANGELOG 记一行。

## 0. 现状（为什么需要这份约定）

- **单文件**：`chatnest-vXX.html`，约 580KB，CSS / JS / DOM 全内联，一个巨型 IIFE。
- **没有分支历史**：`main` 里直接按版本号堆文件，目前 209 个文件、约 60MB，含 40+ 个历史版本 HTML。
- **发布 = 直接改 main**，Pages 自动构建。没有 PR，没有 diff，也没有冲突提示——两个人同时 PUT，后写的覆盖先写的，丢改动不会报错。
- **`git clone` 在国内网络下经常失败**（实测两次 TLS 中断），`tarball` 也要几十分钟。所以默认走 GitHub API 提交，见第 5 节。

## 1. 分工

按**页面**切，不按"视觉 / 结构"切。配色和布局长在同一串 CSS 里，切不开。

| 区块 | 归属 | 说明 |
|---|---|---|
| 聊天主界面：消息流、气泡、输入区、顶栏、dock、悬浮球 | `kern` | dock 与悬浮球是全局动效，跟主界面绑在一起 |
| 二级页：设置、聊天信息 sheet、心潮 iframe、探索页、启动页 | `dsh` | 都是独立 sheet / view，DOM 自成一块 |

**越界改动**（要动对方那块的样式）：先在本文末 CHANGELOG 追加一行，说明动了什么、为什么，再提交。

## 2. 变量分级（52 个 `--*`，这是白名单本体）

### A 级 · 全局骨架 —— **不可改**

改了整页会一起变，肉眼分辨不出是哪一处引起的。

```
--panel  --panel2  --panel3  --panel-solid  --panel-top
--panel-fg  --panel-fg2  --text  --text2  --text3
--line  --line2  --bg0  --bg1  --bg2
--fs  --sans  --serif  --ease  --shadow
--r-full  --r-bubble  --r-panel
```

### B 级 · 品牌与色彩 —— **改前必须登记**

这些直接决定"高级感 / 廉价感"，历史返工全出在这一层。一次只动一个，一次只动一档。

```
--ball-on       悬浮球激活色，15 处引用
--accent  --accent2
--moon
--user-b1  --user-b2  --user-fg  --user-line
--ai-b  --ai-line
--danger
```

### C 级 · 组件局部 —— **各自随便改**

只影响自己那块，不动全局观感。

```
--plus-ico  --plus-tile  --setgroup
--mc-green / -bd / -bg      --mc-red / -bd / -bg
--mc-blue / -bd / -bg       --mc-gray-bd / -bg
--mcp-ico-bg  --mcp-gt  --mcp-dot-on  --mcp-dot-off
```

**新增变量**：只加 `--k-*`（kern）或 `--d-*`（dsh）前缀，不许往 A / B 级里加。

## 3. 版本号

| 谁 | 号段 |
|---|---|
| `kern` | v270 – v279 |
| `dsh` | v280 – v289 |

- 文件名：`chatnest-v45.<号>.html`
- **`index.html`（入口跳转）和 `version.json` 只由 `kern` 改**。别人要发版，告一声版本号，由 kern 统一抬。
- 理由：这两个文件是唯一入口，两个人同时改一定互相覆盖，表现是"页面没更新"。

## 4. 提交

- 单次改动 **≤ 100 行**，一次只说一件事。攒一大坨再提交，冲突时无法判断该留哪边。
- **只加不删**：要改接口字段或 CSS 类名，先加新的、新旧并存、上线；用的一方切过去，稳定后再删旧的。
- 历史版本文件**不删不改**。回滚就是把入口指回上一版。

## 5. 发布（clone 不通时的走法）

```bash
# 读
GET /repos/Eira-hub/chatnest/contents/<path>?ref=main
# 写
PUT /repos/Eira-hub/chatnest/contents/<path>
    { "message": "...", "content": "<base64>", "branch": "main", "sha": "<上一步的 sha>" }
```

`sha` 必须带，不带就是新建；带了但过期会 409——**409 说明有人抢先提交了，停下来重新读，不要重试覆盖**。

分支已建好：`kern/ui`、`dsh/ui`（都从 `main` 拉）。能 clone 的环境优先走分支 + PR；不能 clone 的，直接按上面的 API 走，但提交前必须先 GET 一次拿最新 sha。

## 6. 部署后必做

- Pages 构建有 40–60 秒延迟，验证时带 `?t=<时间戳>` 破缓存。
- 每次上线核对 `version.json` 与实际文件字节数，两者对不上说明构建还没完。

---

## CHANGELOG（越界改动与 B 级变量变更在此登记）

| 时间 | 谁 | 动了什么 | 为什么 |
|---|---|---|---|
| 2026-11-16 | kern | 建立本文件 | 双人并行 UI 协作 |
| 2026-11-17 | dsh | v45.50：设置页/聊天信息页按背景模式分浅色档；`#settingsSheet` 等 sheet 作用域内重定义 --text/--panel-fg 等 A 级变量（非 :root） | 渐变模式下深玻璃压在浅底上，与探索页/会话列表两个世界；局部重定义经仓库主确认 |
| 2026-11-17 | dsh | 代发 v45.50：新增 chatnest-v45.50.html、version.json 抬到 v277、index.html 兜底指向 v45.50 | 仓库主提供临时凭据直接发布；这两个入口文件按约定属 kern，请 kern 复核 |
