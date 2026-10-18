import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx

from .models import ChatMessageRequest, ChildEvent
from .sse import parse_sse

logger = logging.getLogger(__name__)


def _endpoint(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def _message_content(response: dict[str, Any]) -> str:
    choices = response.get("choices") or []
    if not choices:
        raise ValueError("Chat Completions 响应中没有 choices")
    content = choices[0].get("message", {}).get("content")
    if not isinstance(content, str):
        raise ValueError("Chat Completions 响应中没有文本 content")
    return content


class OpenAICompatibleClient:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def classify(
        self,
        query: str,
        config: dict[str, Any],
        workflow_names: list[str],
    ) -> list[str]:
        prompt = str(config["system_prompt"]).replace(
            "{workflow_names}", "、".join(workflow_names)
        )
        payload = {
            "model": config["model"],
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": query},
            ],
            "temperature": config.get("temperature", 0),
            "response_format": {"type": "json_object"},
        }
        try:
            response = await self.client.post(
                _endpoint(config["base_url"], "chat/completions"),
                headers=_headers(config["api_key"]),
                json=payload,
            )
            response.raise_for_status()
            result = json.loads(_message_content(response.json()))
            if isinstance(result, list):
                routes = result
            elif isinstance(result, dict):
                routes = result.get("workflows")
            else:
                routes = None
            if not isinstance(routes, list) or not all(isinstance(x, str) for x in routes):
                raise ValueError("意图识别结果不是字符串列表")
            return routes
        except (
            httpx.HTTPError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            logger.warning("intent_classification_failed error=%s", exc)
            return ["错误"]

    async def stream_chat(
        self,
        query: str,
        config: dict[str, Any],
    ) -> AsyncIterator[str]:
        payload = {
            "model": config["model"],
            "messages": [
                {"role": "system", "content": config["system_prompt"]},
                {"role": "user", "content": query},
            ],
            "temperature": config.get("temperature", 0.7),
            "stream": True,
        }
        async with self.client.stream(
            "POST",
            _endpoint(config["base_url"], "chat/completions"),
            headers=_headers(config["api_key"]),
            json=payload,
        ) as response:
            response.raise_for_status()
            async for _, data in parse_sse(response.aiter_lines()):
                if not data or data == "[DONE]":
                    continue
                chunk = json.loads(data)
                choices = chunk.get("choices") or []
                if not choices:
                    continue
                content = choices[0].get("delta", {}).get("content")
                if content:
                    yield str(content)


class DifyClient:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def stream_chatflow(
        self,
        request: ChatMessageRequest,
        config: dict[str, Any],
        conversation_id: str,
    ) -> AsyncIterator[ChildEvent]:
        payload = {
            "inputs": request.inputs,
            "query": request.query,
            "response_mode": "streaming",
            "conversation_id": conversation_id,
            "user": request.user,
            "files": request.files,
        }
        async with self.client.stream(
            "POST",
            _endpoint(config["base_url"], "chat-messages"),
            headers=_headers(config["api_key"]),
            json=payload,
        ) as response:
            response.raise_for_status()
            async for event_name, data in parse_sse(response.aiter_lines()):
                if not data:
                    continue
                decoded = json.loads(data)
                if not isinstance(decoded, dict):
                    continue
                kind = str(decoded.get("event") or event_name or "unknown")
                yield ChildEvent(event=kind, payload=decoded)

    async def stop(self, config: dict[str, Any], task_id: str, user: str) -> None:
        response = await self.client.post(
            _endpoint(config["base_url"], f"chat-messages/{task_id}/stop"),
            headers=_headers(config["api_key"]),
            json={"user": user},
        )
        response.raise_for_status()
