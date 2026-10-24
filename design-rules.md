# 星夜小筑 · 设计规则（内部参考）

> 来源：pbakaus/impeccable · Leonxlnx/taste-skill v2 · emilkowalski/skills(emil-design-eng) · vercel-labs/web-interface-guidelines
> 适用：chatnest 单文件前端（移动优先、手写 CSS、token 化）。产品形态 = **Operate 模式**（用户在里面干活：聊天、组队、设置）。

## 一、总纲（Impeccable）

- **Operate 模式**：可扫读、一致性、原生预期 > 表现欲。品牌感活在精准的细节里，不靠装饰。
- **精修保留 / 重设计才换皮**：改 UI 前先问这是精修还是重设计；精修必须保留现有身份、行为、文案，不越界。动用户的东西先问。
- **渲染验证**：改完必须看真实渲染效果（手机 375px + 桌面宽度），批量改完再截图，别改一下看一眼。
- 上线前过一遍 polish + harden：错误态、空态、边界（超长昵称、断网、历史为空）。

## 二、品味基调（taste-skill · app-screen 规则）

- 安静的边框、小号标签、重复面板 = 正常；**渐变光晕、图标底色块、彩虹图表、装饰性数字 = 禁止**（星空背景是品牌例外，仅限背景层）。
- 所有颜色/字号/圆角/动效只引用 `--token`，不写裸值。
- 克制不等于空洞：拿走装饰的同时要补实质（内容密度、层级、留白节奏）。

## 三、动效手感（emil）

**动效频率决策**：每天用几十次以上的操作（发消息、点气泡）不加动画或极短；偶发操作（面板、toast）标准动画；首次才见的（引导）可以加彩。

| 场景 | 参数 |
|---|---|
| 所有可按压元素 `:active` | `transform: scale(0.97)`，`transition: transform 160ms ease-out` |
| 气泡/面板入场 | `opacity 0 + scale(0.95)`（**永远不用 scale(0)**），`cubic-bezier(0.23,1,0.32,1)`，200–250ms |
| 抽屉/加号面板/设置页 | `translateY(100%)` 进出，`cubic-bezier(0.32,0.72,0,1)`，300–400ms |
| popover（加号菜单） | 从触发它的按钮缩放进入：`transform-origin` 指向加号位置；模态才用 center |
| toast | 进出同一方向；transition 而非 keyframes（可被打断）；可用 `@starting-style` |
| 悬停/颜色 | `ease`；匀速（进度） | `linear`；**禁止 ease-in**（UI 动画一律 ease-out 起步） |
| 交叉渐变发飘 | 过渡期加 `filter: blur(2px)` 掩饰（≤2px，Safari 贵） |

- UI 动画一律 < 300ms；`transition` 明确列属性，**禁止 `transition: all`**。
- 三点跳动气泡属高频元素：保持现有 1.2s 循环即可，不加进场动画。

## 四、硬性审查清单（Vercel，上线前逐条过）

- 无障碍：图标按钮必须有 `aria-label`；表单有 label；toast 用 `aria-live="polite"`；语义标签优先于 ARIA。
- 焦点：可交互元素有 `:focus-visible` 样式；禁止裸 `outline: none`。
- 触控：`touch-action: manipulation`；弹层 `overscroll-behavior: contain`；点按目标 ≥ 44px。
- 动效：`prefers-reduced-motion` 降级（三点、星空闪烁、卡片轮询高亮都要有降级）。
- 文案：`…` 不用 `...`；弯引号；`10 MB`、房码用不换行空格；数字列 `tabular-nums`；错误信息带下一步动作。
- 长内容：气泡文本 `break-words`；flex 子元素 `min-w-0`；空态有兜底 UI。
- 深浅主题：`<meta name="theme-color">` 随主题；`color-scheme` 正确；原生控件显式设 background/color。
- 安全区：全出血布局吃 `env(safe-area-inset-*)`（聊天输入条已处理，新面板要跟）。
- 破坏性操作：先确认或可撤销，绝不立即生效（退出房间已有确认，后续同类照此）。

## 五、执行方式

1. 改 UI 前先声明：精修 or 重设计；涉及身份类变更（配色、质感、结构）先问。
2. 改完用浏览器在 375px 和桌面宽度各截图验证，批量修完再看。
3. 每次涉及动效的新交互，对照第三节表格选参数，不现造曲线。
4. 发版前过第四节清单，结果一句话汇报（过了 / 哪几条没过）。
