"""复制为 config.py 后，按实际环境填写。不要提交真实 API Key。"""

# 网关自身配置。留空表示开发阶段不校验调用方 Authorization。
GATEWAY_CONFIG = {
    "api_key": "change-me",
    "heartbeat_interval_seconds": 15,
    "connect_timeout_seconds": 10,
    "read_timeout_seconds": 300,
}

# OpenAI Chat Completions 兼容接口，用于意图识别。
# 接口应支持 response_format={"type": "json_object"}。
# 只有 {workflow_names} 是动态占位符，提示词中的普通 JSON 花括号无需转义。
INTENT_CONFIG = {
    "base_url": "https://example.com/v1",
    "api_key": "intent-api-key",
    "model": "your-model",
    "system_prompt": (
        "你是请求路由器。根据用户问题，从以下工作流名称中选择一个或多个："
        "{workflow_names}。只返回 JSON，不要解释。推荐格式为 "
        "{\"workflows\":[\"子工作流A\",\"子工作流B\"]}。"
        "若属于闲聊，返回 {\"workflows\":[\"闲聊\"]}。"
    ),
    "temperature": 0,
}

# OpenAI Chat Completions 兼容接口，用于闲聊和问题引导。
CHAT_CONFIG = {
    "base_url": "https://example.com/v1",
    "api_key": "chat-api-key",
    "model": "your-model",
    "system_prompt": (
        "你是友好的业务助手。对日常寒暄做简洁回复；如果用户的问题可能与业务"
        "工作流有关，引导用户补充具体场景、对象和希望解决的问题。"
    ),
    "temperature": 0.7,
}

# key 必须与意图识别输出的名称完全一致；顺序以意图模型返回顺序为准。
WORKFLOW_CONFIGS = {
    "子工作流A": {
        "title": "子工作流A",
        "base_url": "https://dify-a.example.com/v1",
        "api_key": "app-api-key-a",
    },
    "子工作流B": {
        "title": "子工作流B",
        "base_url": "https://dify-b.example.com/v1",
        "api_key": "app-api-key-b",
    },
}
