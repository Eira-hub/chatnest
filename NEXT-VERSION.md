# ChatNest FC MCP — 下一版待办（2026-10-22-r2-mcp 审结后记录）

审结：霁 通过，可出包。md5 一致；静态审安全面无阻塞（剥离/鉴权/校验/截断/轮数上限均在）；
diff 打上基线跑冒烟脚本 SMOKE OK 全绿。

## 霁的四条建议（不阻塞，下一版落实）

1. **同名工具去重静默**：同名工具被 index 去重时目前静默 `continue`，加 `log.warning`
   把被去重的服务器/工具名打出来。
2. **MCP_MAX_ROUNDS 到顶无提示**：轮数上限用尽后模型返回空回复，用户侧无感知；
   到顶时应在 SSE 里给一条提示（error 事件或正文尾注）。
3. **死代码**：`mcp_servers_post` 里 `payload.pop("connected")` 无任何作用，删掉。
4. **冒烟补截断用例**：smoke_mcp.py 增加「工具结果 > 8000 字符被截断」的路径覆盖。

## 真机实测提醒（出包上传 FC 后）

- 前端实测时顺带验证 **aliyun 兼容模式 `tools` + `enable_thinking` 组合**——
  冒烟的 mock 上游覆盖不了真机行为，重点看：注入 tools 后流式是否仍正常、
  thinking 分片与 tool_calls 分片混流时解析是否正确。
- 端到端：前端 MCP 管理页加一个真实 MCP 服务器（如高德 / fetch），
  聊天触发真实调用，确认对话流内工具胶囊（SSE `tool` 事件）正常亮起。

## 版本对照

- 前端：v40.3（cache v=122），MCP 管理页契约 = 后端 `/api/mcp/*` 已对齐
- 后端：2026-10-22-r2-mcp（diff 基线 = 原包 app.py 1548 行）
- 文件：fc-mcp-20261022-r2.diff（md5 604787fa1411956b944f629550929de6）、
  smoke_mcp.py（md5 cf65810e7f990f2136c7498e25acf812）
- 存放：GitHub Eira-hub/room-mcp-kit @ chatnest-fc-mcp-review（提交 9b914e8）
