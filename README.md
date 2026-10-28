# FakeDifyAPI

基于 FastAPI 的 Dify Chatflow 兼容网关。它先通过 OpenAI Chat Completions
兼容接口识别意图，再按识别顺序串行调用多个 Dify Chatflow，并将文本与引用
聚合成一条 Dify 风格的 SSE 响应。

## 快速开始

1. 复制 `src/config_example.py` 为 `src/config.py`（当前工作区已生成一份），
   再填写网关、意图识别、闲聊和子工作流配置。`src/config.py` 已被 Git 忽略，
   避免误提交真实 API Key；缺少该文件时服务会回退到示例配置。
2. 启动开发容器：

   ```bash
   docker compose up --build
   ```
   或者直接干：
   ```bash
   python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000 
   ```
3. 调用接口：

   ```bash
   curl -N http://localhost:8000/v1/chat-messages \
     -H "Authorization: Bearer change-me" \
     -H "Content-Type: application/json" \
     -d '{"inputs":{},"query":"你好","response_mode":"streaming","conversation_id":"","user":"demo","files":[]}'
   ```

开发容器通过 `uvicorn --reload --host 0.0.0.0 --port 8000` 启动，且
`./src` 挂载到容器 `/app/src`，修改代码后会自动重载。

## 当前实现

- `POST /v1/chat-messages`：仅支持 `response_mode=streaming`。
- `POST /v1/chat-messages/{task_id}/stop`：停止当前子 Chatflow，并阻止后续调用。
- `GET /health`：健康检查。
- 意图结果支持 JSON 裸列表及 `{"workflows": [...]}`，异常统一转为 `["错误"]`。
- `["闲聊"]` 走单独的兼容接口和指定提示词。
- 多工作流严格串行调用；单个工作流失败不影响后续工作流。
- 公共 SSE 自行发送心跳，只发送一组公共 ID 和一个最终 `message_end`。
- 流式回答默认以 7 个文本单位/秒起步平滑输出；默认自适应模式会在积压时加速，严格模式固定速率。单位采用与模型无关的分词近似，不代表上游模型的精确 token 数。

当前会话与任务映射保存在单进程内存中；部署多个 worker 前需要换成共享存储。

流式节奏通过 `GATEWAY_CONFIG` 配置：`output_pacing_enabled` 控制开关，
`output_pacing_mode` 可设为 `adaptive` 或 `strict`，`output_tokens_per_second`
默认是 7。`output_max_tokens_per_second`、`output_backlog_threshold` 和
`output_buffer_units` 分别控制自适应上限、积压阈值和有界缓冲。

## 设计文档

项目目标是实现一个基于 FastAPI 的 Dify Chatflow 兼容网关：接收一次 `/v1/chat-messages` 请求，根据路由结果依次调用多个业务 Chatflow，将各子 Chatflow 的文本流拼接为一条公共 SSE 响应，并在全部子调用结束后统一返回引用资源。

## 第一版约束

- 已确认技术栈：FastAPI。
- 已确认第一阶段调用模式：多个业务 Chatflow 串行调用、串行输出。
- 已确认输出形式：每个子 Chatflow 以前置 Markdown 标题分区。
- 已确认失败策略：单个子 Chatflow 失败时输出简短错误提示，其余子 Chatflow 继续执行，公共调用正常结束。
- 已确认引用策略：仅展示引用；按子 Chatflow 执行顺序汇总，并在公共 `message_end` 中一次性返回。
- 暂无可用于联调的真实 Dify Chatflow；开发期间使用模拟上游和录制的 SSE 样例验证协议行为。
- 当前只实现串行流式版本，多进程共享状态与并行调用留待后续版本。

## 文档索引

- [系统设计](./architecture.md)：目标、边界、Chatflow 协议、事件聚合、ID 管理、异常处理和测试策略。
- [开发路线](./development-roadmap.md)：分阶段实施内容、验收标准和后续并行化方向。

## 已确认的接口范围

第一阶段聚焦 Chatflow，而不是 Workflow：

```text
POST /v1/chat-messages
POST /v1/chat-messages/{task_id}/stop
```

第一阶段必须支持：

- `response_mode=streaming`
- Dify 风格 SSE 帧
- `message`、`message_end`、`error`、`ping`
- Chatflow 公共 `workflow_started` / `workflow_finished` 生命周期
- 多子 Chatflow 串行调用
- 引用资源聚合
- 部分成功
- 公共会话及任务 ID 映射

阻塞响应、文件上传、会话列表、消息历史、反馈和推荐问题不属于第一阶段的已确认范围，是否实现将在开发规范和接口范围讨论后决定。
