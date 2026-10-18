import json
from typing import Any

import httpx
import pytest

from src.clients import DifyClient, OpenAICompatibleClient
from src.models import ChatMessageRequest, ChildEvent
from src.orchestrator import (
    _clean_resources,
    _collect_usage,
    _safe_routes,
    stream_response,
)
from src.sse import encode_data, parse_sse


@pytest.mark.asyncio
async def test_sse_parser_handles_multiline_and_final_frame() -> None:
    async def lines():
        for line in ["event: custom", "data: {\"a\":", "data: 1}", "", "data: end"]:
            yield line

    result = [item async for item in parse_sse(lines())]
    assert result == [("custom", '{"a":\n1}'), (None, "end")]


@pytest.mark.asyncio
async def test_classifier_only_replaces_workflow_names_placeholder() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        prompt = payload["messages"][0]["content"]
        assert "子工作流A、子工作流B" in prompt
        assert '{"workflows":["子工作流A"]}' in prompt
        return httpx.Response(
            200,
            json={
                "choices": [
                    {"message": {"content": '{"workflows":["子工作流A"]}'}}
                ]
            },
        )

    config = {
        "base_url": "https://llm.example/v1",
        "api_key": "test-key",
        "model": "test-model",
        "system_prompt": (
            "候选：{workflow_names}。输出示例："
            '{"workflows":["子工作流A"]}'
        ),
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await OpenAICompatibleClient(client).classify(
            "问题", config, ["子工作流A", "子工作流B"]
        )
    assert result == ["子工作流A"]


def test_encode_data_keeps_chinese() -> None:
    encoded = encode_data({"event": "message", "answer": "你好"}).decode()
    assert "你好" in encoded
    assert json.loads(encoded.removeprefix("data: "))["answer"] == "你好"


def test_routes_are_whitelisted_and_deduplicated() -> None:
    assert _safe_routes(["子工作流A", "不存在", "子工作流A", "子工作流B"]) == [
        "子工作流A",
        "子工作流B",
    ]
    assert _safe_routes(["子工作流A", "错误"]) == ["错误"]
    assert _safe_routes(["不存在"]) == ["错误"]


def test_resources_and_usage_are_aggregated() -> None:
    usage = {}
    _collect_usage(
        usage,
        {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
    )
    _collect_usage(
        usage,
        {"prompt_tokens": 7, "completion_tokens": 11, "total_tokens": 18},
    )
    assert usage["total_tokens"] == 23
    assert _clean_resources([{"position": 9, "message_id": "child"}]) == [
        {"position": 1}
    ]


@pytest.mark.asyncio
async def test_error_route_still_has_single_public_end_event(monkeypatch: Any) -> None:
    async def classify(*args: Any, **kwargs: Any) -> list[str]:
        return ["错误"]

    monkeypatch.setattr(OpenAICompatibleClient, "classify", classify)
    request = ChatMessageRequest(
        inputs={}, query="test", response_mode="streaming", user="tester"
    )
    raw = b"".join([chunk async for chunk in stream_response(request)]).decode()
    payloads = [
        json.loads(line.removeprefix("data: "))
        for line in raw.splitlines()
        if line.startswith("data: ")
    ]
    events = [payload["event"] for payload in payloads]
    assert events.count("message_end") == 1
    assert events.count("workflow_finished") == 1
    public_ids = {
        payload["message_id"]
        for payload in payloads
        if "message_id" in payload
    }
    assert len(public_ids) == 1


@pytest.mark.asyncio
async def test_workflows_stream_serially_and_merge_resources(monkeypatch: Any) -> None:
    async def classify(*args: Any, **kwargs: Any) -> list[str]:
        return ["子工作流A", "子工作流B"]

    call_order: list[str] = []

    async def child_stream(
        self: DifyClient,
        request: ChatMessageRequest,
        config: dict[str, Any],
        conversation_id: str,
    ):
        name = "A" if config["title"] == "子工作流A" else "B"
        call_order.append(name)
        yield ChildEvent(
            event="message",
            payload={
                "event": "message",
                "task_id": f"task-{name}",
                "conversation_id": f"conversation-{name}",
                "answer": f"answer-{name}",
            },
        )
        yield ChildEvent(
            event="message_end",
            payload={
                "event": "message_end",
                "metadata": {
                    "usage": {"total_tokens": 1},
                    "retriever_resources": [
                        {"position": 99, "document_name": f"doc-{name}"}
                    ],
                },
            },
        )
        yield ChildEvent(event="workflow_finished", payload={"event": "workflow_finished"})

    monkeypatch.setattr(OpenAICompatibleClient, "classify", classify)
    monkeypatch.setattr(DifyClient, "stream_chatflow", child_stream)
    request = ChatMessageRequest(
        inputs={}, query="test", response_mode="streaming", user="tester"
    )
    raw = b"".join([chunk async for chunk in stream_response(request)]).decode()
    payloads = [
        json.loads(line.removeprefix("data: "))
        for line in raw.splitlines()
        if line.startswith("data: ")
    ]

    answers = [item["answer"] for item in payloads if item["event"] == "message"]
    assert call_order == ["A", "B"]
    assert answers == [
        "### 子工作流A\n\n",
        "answer-A",
        "\n\n### 子工作流B\n\n",
        "answer-B",
    ]
    end = next(item for item in payloads if item["event"] == "message_end")
    assert [
        item["position"] for item in end["metadata"]["retriever_resources"]
    ] == [1, 2]
    assert end["metadata"]["usage"]["total_tokens"] == 2
