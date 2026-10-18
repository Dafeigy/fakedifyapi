# FakeDifyAPI 系统设计

## 1. 背景

现有业务包含多个独立的 Dify Chatflow。一次用户问题可能同时命中多个业务能力，例如“如何开启校园招聘项目”可能同时需要：

- 企业管理规定：提供项目立项、审批、预算和制度要求。
- 人事助手：提供招聘计划、岗位编制和校园招聘执行流程。

Dify Chatflow 直接调用时能够流式输出，并在 `message_end.metadata.retriever_resources` 中返回知识库引用。但如果在另一个 Chatflow 内通过 HTTP 或工具节点嵌套调用业务 Chatflow，子应用的 SSE 文本流和引用元数据不会自然提升为父 Chatflow 的公共响应。

本项目在 Dify 之外增加一个兼容网关，负责路由、调用和聚合。

## 2. 目标

第一阶段目标如下：

1. 对调用方暴露与 Dify Chatflow 相同风格的 `/v1/chat-messages` 接口。
2. 支持一次请求路由到多个业务 Chatflow。
3. 按路由顺序串行调用业务 Chatflow，并实时输出当前子 Chatflow 的文本。
4. 使用 Markdown 标题区分不同业务回答。
5. 截留每个子 Chatflow 的 `message_end`，汇总全部成功调用的引用资源。
6. 全部子调用完成后只发送一个公共 `message_end`。
7. 单个子 Chatflow 失败不终止整条回答，以普通文本提示失败并继续执行后续子 Chatflow。
8. 为未来“并行调用、按路由顺序回放”保留扩展边界。

## 3. 非目标

第一阶段不处理以下需求：

- 将引用资源标记为来自哪个子 Chatflow。
- 修改或重写子回答中的引用编号；业务 Chatflow 应移除正文引用标识。
- 综合、改写或消除多个子回答之间的重复内容。
- `message_replace` 的聚合语义。
- 多子 Chatflow 并行执行。
- 将 Dify Workflow `/v1/workflows/run` 作为公共接口。
- 完整复制 Dify 的所有应用管理、会话管理和文件接口。

## 4. Chatflow 协议边界

本项目聚合的是 Dify Chatflow，而不是 Workflow。

### 4.1 公共请求

公共端点：

```text
POST /v1/chat-messages
Content-Type: application/json
Authorization: Bearer <gateway-api-key>
```

核心请求字段：

```json
{
  "inputs": {},
  "query": "如何开启校园招聘项目？",
  "response_mode": "streaming",
  "conversation_id": "",
  "user": "user-123",
  "files": []
}
```

第一阶段以 `response_mode=streaming` 为主要实现目标。

### 4.2 公共响应

公共响应使用 `text/event-stream`。一个典型响应如下：

```text
event: ping

data: {"event":"workflow_started","task_id":"public-task","workflow_run_id":"public-run","message_id":"public-message","conversation_id":"public-conversation","created_at":1700000000,"data":{...}}

data: {"event":"message","task_id":"public-task","message_id":"public-message","conversation_id":"public-conversation","answer":"### 企业管理规定\n\n","created_at":1700000000}

data: {"event":"message","task_id":"public-task","message_id":"public-message","conversation_id":"public-conversation","answer":"回答片段……","created_at":1700000001}

data: {"event":"message","task_id":"public-task","message_id":"public-message","conversation_id":"public-conversation","answer":"\n\n### 人事助手\n\n","created_at":1700000002}

data: {"event":"message","task_id":"public-task","message_id":"public-message","conversation_id":"public-conversation","answer":"回答片段……","created_at":1700000003}

data: {"event":"message_end","task_id":"public-task","message_id":"public-message","conversation_id":"public-conversation","created_at":1700000004,"metadata":{"usage":{...},"retriever_resources":[...]}}

data: {"event":"workflow_finished","task_id":"public-task","workflow_run_id":"public-run","message_id":"public-message","conversation_id":"public-conversation","created_at":1700000004,"data":{"id":"public-run","status":"succeeded",...}}
```

### 4.3 子 Chatflow 完成条件

Chatflow 的 `message_end` 和 `workflow_finished` 含义不同：

- `message_end`：本轮对话消息结束，包含 usage 和引用资源。
- `workflow_finished`：本次 Chatflow 底层流程执行结束。

网关在收到子 `message_end` 时保存引用和用量，但应继续读取，直到收到子 `workflow_finished`、明确错误或流正常结束，之后才能启动下一个串行子调用。

## 5. 总体架构

```text
调用方
  │
  │ POST /v1/chat-messages
  ▼
FastAPI 接口层
  │
  ├── 鉴权与请求校验
  ├── 创建公共 conversation/message/task/run ID
  └── 打开公共 SSE 响应
          │
          ▼
      路由服务
          │ routes: [企业管理规定, 人事助手]
          ▼
    串行 Chatflow 编排器
          │
          ├── 输出章节标题
          ├── 调用当前子 Chatflow
          ├── 转发 message.answer
          ├── 截留 message_end
          ├── 等待 workflow_finished
          └── 继续下一个路由
          │
          ▼
      引用/用量聚合器
          │
          ├── 生成公共 message_end
          └── 生成公共 workflow_finished
```

## 6. 组件职责

### 6.1 FastAPI 接口层

- 暴露兼容端点。
- 校验调用方 API Key。
- 解析并验证 Chatflow 请求。
- 创建公共响应所需 ID。
- 返回异步 SSE 流。
- 感知客户端断开并通知编排器停止。

### 6.2 路由服务

输入用户问题，调用 OpenAI Chat Completions 兼容接口，以 JSON format 输出有序、
多标签工作流名称列表。推荐返回对象以兼容 `json_object` 模式，网关也兼容裸数组：

```json
{"workflows": ["企业管理规定", "人事助手"]}
```

约束：

- 只接受配置中存在的工作流名称。
- 保留路由返回顺序，作为调用和输出顺序。
- 路由结果不能直接携带任意 URL 或密钥。
- 判断为闲聊时返回 `["闲聊"]`，使用独立提示词流式回复并引导提问。
- 请求、协议或解析失败时内部统一返回 `["错误"]`，公共流输出友好错误文本。

意图识别接口不保留 Dify Chatflow 会话，仅使用当前用户问题进行判断。

### 6.3 Dify Chatflow 客户端

计划使用异步 HTTP 客户端连接业务 Chatflow，并以 SSE 帧为边界解析响应。

职责：

- 将公共请求转换为子 Chatflow 请求。
- 注入目标 Chatflow 的 URL 和 API Key。
- 强制使用 `response_mode=streaming`。
- 正确处理 `event: ping` 和 `data: <json>`。
- 不能把任意 TCP 数据块当作完整 JSON。
- 暴露统一的内部事件模型。
- 支持超时、取消和资源释放。

### 6.4 串行编排器

对每个路由依次执行：

1. 检查公共任务是否已取消。
2. 输出 `\n\n### {title}\n\n`。首个标题可不包含前导空行。
3. 查找该公共会话对应的子 `conversation_id`。
4. 调用子 Chatflow。
5. 将子 `message.answer` 转换为使用公共 ID 的 `message` 事件。
6. 截留子 `message_end`，保存引用和用量。
7. 等待子 `workflow_finished` 或流结束。
8. 保存该子 Chatflow 返回的 `conversation_id`。
9. 继续下一个路由。

所有路由完成后：

1. 发送唯一公共 `message_end`。
2. 发送唯一公共 `workflow_finished`。
3. 清理运行中任务映射。

### 6.5 状态存储

需要保存两类映射。

会话映射：

```text
public_conversation_id
  ├── router -> router_conversation_id（如启用）
  ├── enterprise_policy -> child_conversation_id_a
  └── hr_assistant -> child_conversation_id_b
```

运行任务映射：

```text
public_task_id
  ├── status
  ├── current_route
  ├── current_child_task_id
  └── cancelled
```

单进程开发阶段可以使用内存实现；生产环境若运行多个实例，应使用共享存储。具体实现将在开发规范和部署规范确定后决定。

## 7. ID 策略

### 7.1 `conversation_id`

- 第一次公共调用未传或传空值时，由网关创建公共 `conversation_id`。
- 网关为每个公共会话保存各子 Chatflow 的 `conversation_id`。
- 后续命中已有子 Chatflow 时，使用已保存的子会话 ID。
- 首次命中某个子 Chatflow 时传空值，并保存其返回的子会话 ID。
- 公共事件永远只暴露公共 `conversation_id`。

### 7.2 `message_id`

- 每次公共 `/chat-messages` 请求创建一个新的公共 `message_id`。
- 同一次公共响应内所有 `message` 和 `message_end` 使用相同公共 `message_id`。
- 子 `message_id` 仅用于内部日志和排障。

### 7.3 `task_id`

- 每次公共生成创建一个新的公共 `task_id`。
- 子 Chatflow 每次调用会产生自己的 `task_id`。
- 公共停止接口根据公共 `task_id` 找到当前子任务并转发停止请求。
- 停止后不得继续启动剩余路由。

### 7.4 `workflow_run_id`

- 网关为公共复合执行创建公共 `workflow_run_id`。
- 子 Chatflow 的 `workflow_run_id` 只用于内部可观测性。
- 子工作流和节点生命周期事件不直接对外转发。

## 8. 子事件处理矩阵

| 子 Chatflow 事件 | 第一阶段处理方式 |
|---|---|
| `ping` | 截留；公共连接由网关自行发送心跳 |
| `workflow_started` | 截留并记录子 `workflow_run_id` |
| `node_started` / `node_finished` / 循环事件 | 截留，可记录调试日志 |
| `message` | 立即转发 `answer`，所有 ID 改为公共 ID |
| `message_end` | 截留，收集引用和 usage |
| `workflow_finished` | 截留，用于判断当前子调用完成 |
| `error` | 转换为当前章节中的普通错误提示，并继续后续路由 |
| `message_replace` | 第一阶段不支持；行为将在实现前明确 |
| `message_file` | 第一阶段是否支持尚未确认 |
| `reasoning_chunk` | 默认不对外暴露，后续按需求决定 |
| Human Input 事件 | 第一阶段不支持嵌套子 Chatflow 暂停 |

## 9. 引用资源聚合

### 9.1 收集时机

引用仅在各子 Chatflow 的 `message_end.metadata.retriever_resources` 到达后收集。公共连接在全部子调用结束前不发送 `message_end`。

### 9.2 聚合规则

第一阶段采用最保守规则：

1. 只收集成功产生完整 `message_end` 的子调用引用。
2. 按路由执行顺序拼接。
3. 保持各子 Chatflow 内部引用顺序。
4. 将公共 `position` 重新编号为从 1 开始的连续整数。
5. 保持 Dify 现有字段结构。
6. 不添加子 Chatflow 来源字段。
7. 第一阶段不跨 Chatflow 去重。

可能出现的引用字段包括：

```json
{
  "position": 1,
  "dataset_id": "...",
  "dataset_name": "...",
  "document_id": "...",
  "document_name": "...",
  "segment_id": "...",
  "score": 0.91,
  "content": "..."
}
```

如上游返回 `id`、`message_id` 等与子消息绑定的附加字段，公共响应应删除这些可选字段或改写为公共消息语义，不能泄漏会造成错误关联的子消息 ID。具体保留字段以目标 Dify 版本的实际响应样例为准。

### 9.3 无引用情况

- 子 Chatflow 没有引用时追加空列表。
- 所有子 Chatflow 都没有引用时，公共 `retriever_resources` 返回空数组。
- 子调用失败且没有完整 `message_end` 时，不使用其不完整引用状态。

## 10. Usage 聚合

建议规则：

- `prompt_tokens`：具有完整 usage 的子调用之和。
- `completion_tokens`：具有完整 usage 的子调用之和。
- `total_tokens`：汇总后的 prompt 与 completion 之和。
- `total_price`：仅在币种一致且字段可可靠转换时求和。
- `currency`：全部可计价子调用一致时保留。
- `latency`：公共请求的实际墙钟耗时，而不是盲目相加子 latency。

若不同 Dify 版本的 usage 字段存在差异，聚合器应保留兼容的已知字段，并允许未知字段被忽略。

## 11. 部分成功策略

单个子 Chatflow 失败时，不发送公共 `error`，因为该事件通常会使客户端结束整个响应。

### 11.1 产生文本前失败

在章节标题后输出普通 `message`：

```text
该服务暂时无法响应，请稍后重试。
```

### 11.2 已产生部分文本后失败

保留已经发送的内容，并追加：

```text


> 本模块回答未完整生成，请稍后重试。
```

### 11.3 公共结束状态

只要网关完成了所有路由的处理和错误降级，公共 `workflow_finished` 建议返回 `status=succeeded`，以保持常见 Chatflow 客户端行为稳定。

内部日志必须记录：

- 公共 task/message/conversation/run ID。
- 失败路由。
- 子 task/conversation/workflow run ID。
- HTTP 状态、Dify 错误码和异常信息。
- 是否在产生部分文本后失败。

## 12. 心跳、超时与连接

串行调用会累计总耗时，因此公共 SSE 连接必须由网关独立维护心跳，不能依赖当前子 Chatflow 的 ping。

要求：

- 打开连接后尽快发送首个 `event: ping`。
- 长时间没有公共数据时定期发送 `event: ping`。
- 响应头至少包含：

```text
Content-Type: text/event-stream
Cache-Control: no-cache
X-Accel-Buffering: no
```

- 反向代理关闭响应缓冲。
- 读取超时覆盖多个串行 Chatflow 的最坏总时长。
- 客户端断开后取消当前上游请求，并阻止后续路由启动。
- 一旦已经向客户端输出正文，不自动重试可能产生重复文本的子流；是否重试只允许发生在首个文本事件之前。

## 13. 停止生成

公共端点：

```text
POST /v1/chat-messages/{public_task_id}/stop
```

停止流程：

1. 验证请求中的 `user` 与原请求一致。
2. 将公共任务标记为取消。
3. 如果当前子 Chatflow 已产生 `task_id`，调用其停止接口。
4. 关闭或取消当前上游流。
5. 不再启动剩余子 Chatflow。
6. 以目标 Dify 版本允许的方式结束公共流。

停止后的最终事件细节需要在获得真实 Dify 样例后固定。

## 14. 配置和安全

配置集中在 `src/config.py`，示例结构见 `src/config_example.py`。业务路由使用
字典保存，key 必须与意图识别返回的名称一致：

```python
WORKFLOW_CONFIGS = {
    "企业管理规定": {
        "title": "企业管理规定",
        "base_url": "https://dify.example.com/v1",
        "api_key": "app-api-key",
    },
}
```

同一文件还包含 `GATEWAY_CONFIG`、`INTENT_CONFIG` 和 `CHAT_CONFIG`，分别配置
网关鉴权/超时、意图识别兼容接口，以及闲聊兼容接口与提示词。

安全要求：

- API Key 只保存在服务端配置或密钥系统中。
- 路由模型只能返回已注册的路由 ID。
- 禁止将模型输出直接作为请求 URL。
- 日志不得记录 API Key、Authorization 头和完整敏感输入。
- 根据部署环境限制可访问的 Dify 主机，避免 SSRF。

`src/config.py` 被 Git 忽略，生产环境必须通过安全渠道将其挂载或注入容器；
不得使用示例密钥直接对外提供服务。

## 15. 可观测性

每次公共调用应建立一条完整链路：

```text
public request
  ├── route decision
  ├── child call 1
  ├── child call 2
  └── public completion
```

建议采集：

- 路由耗时和路由结果。
- 首个公共正文字符耗时。
- 每个子 Chatflow 的连接、首字和完成耗时。
- 公共总耗时。
- 子调用成功率和部分成功率。
- 引用数量。
- Token 和费用汇总。
- 客户端主动断开和停止次数。

日志使用公共 ID 关联，子 ID 作为结构化字段保存。

## 16. 暂无真实 Dify 环境时的测试策略

缺少真实业务 Chatflow 不阻塞网关主体开发。第一阶段使用可重复的模拟 SSE 上游。

### 16.1 模拟 Chatflow 服务

模拟服务需要覆盖：

- 正常 `ping -> workflow_started -> message* -> message_end -> workflow_finished`。
- 多个引用资源。
- 无引用资源。
- 首字前 HTTP 失败。
- 首字后 SSE `error`。
- 流中断。
- 长时间无文本但持续 ping。
- `message_end` 很大并被拆成多个网络数据块。
- 用户停止任务。
- 同一公共会话多轮调用。

### 16.2 协议夹具

将脱敏后的 Dify SSE 响应保存为测试夹具，测试解析器不能依赖 TCP chunk 边界，而必须依赖 SSE 帧边界。

在后续获得真实 Dify 环境后，应补充：

- 目标 Dify 版本。
- 一个无知识库的 Chatflow。
- 一个有知识库且开启引用的 Chatflow。
- 一份真实 streaming 响应。
- 一份失败响应。
- 一份停止生成响应。

真实样例用于锁定可选字段、事件顺序、停止语义和版本差异。

## 17. 后续并行化方向

未来可以在不改变公共协议的前提下升级为：

1. 按路由顺序同时启动多个子 Chatflow。
2. 每个子 Chatflow 写入独立、有界的事件队列。
3. 公共输出端先消费第一路队列。
4. 第一条路由完成后，按原始事件顺序回放第二路已缓存的 `message`。
5. 对尚未完成的路由继续实时消费。
6. 全部完成后按相同规则汇总引用并发送公共 `message_end`。

并行版本需要额外解决：

- 有界内存和磁盘溢写。
- 较慢客户端导致的背压。
- 多上游任务统一取消。
- 已完成后续路由的事件缓存清理。
- 进程异常时的任务恢复策略。

串行版本应提前抽象“子事件生产者”和“公共事件消费者”，避免未来并行化时重写 SSE 协议层。

## 18. 待开发规范确定的事项

- Python 版本和依赖管理工具。
- 项目目录结构。
- 配置加载方式。
- 状态存储选型。
- 日志和追踪标准。
- 测试框架及覆盖率要求。
- 格式化、类型检查和 lint 工具。
- Docker、部署和 CI 规范。
- API 错误结构和错误码命名。
- 是否支持 blocking 模式及其他 Dify 端点。
