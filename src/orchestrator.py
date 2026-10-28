import asyncio
import contextlib
import json
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from .clients import DifyClient, OpenAICompatibleClient
from .models import ChatMessageRequest
from .settings import CHAT_CONFIG, GATEWAY_CONFIG, INTENT_CONFIG, WORKFLOW_CONFIGS
from .sse import encode_data, encode_ping
from .state import ActiveTask, state

logger = logging.getLogger(__name__)
_END = object()
_TOKEN_PATTERN = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]|"
    r"[\w]+(?:['’][\w]+)*|\s+|[^\w\s]",
    re.UNICODE,
)
_OUTPUT_QUEUE_MAXSIZE = 512


def _text_units(text: str) -> Iterator[str]:
    """Split text into lossless, model-independent pacing units.

    CJK characters and punctuation count as one unit; whitespace runs and
    alphanumeric words count as one. This is a readable-output proxy, not a
    tokenizer for any particular upstream model.
    """
    position = 0
    for match in _TOKEN_PATTERN.finditer(text):
        if match.start() > position:
            yield text[position : match.start()]
        yield match.group()
        position = match.end()
    if position < len(text):
        yield text[position:]


def _public_ids(conversation_id: str) -> dict[str, str]:
    return {
        "conversation_id": conversation_id or str(uuid.uuid4()),
        "message_id": str(uuid.uuid4()),
        "task_id": str(uuid.uuid4()),
        "workflow_run_id": str(uuid.uuid4()),
    }


def _base_event(kind: str, ids: dict[str, str]) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event": kind,
        "task_id": ids["task_id"],
        "message_id": ids["message_id"],
        "conversation_id": ids["conversation_id"],
        "created_at": int(time.time()),
    }
    return event


def _message(ids: dict[str, str], answer: str) -> dict[str, Any]:
    return {**_base_event("message", ids), "answer": answer}


def _safe_routes(raw_routes: list[str]) -> list[str]:
    if "错误" in raw_routes:
        return ["错误"]
    if "闲聊" in raw_routes:
        return ["闲聊"]
    seen: set[str] = set()
    routes: list[str] = []
    for route in raw_routes:
        if route in WORKFLOW_CONFIGS and route not in seen:
            seen.add(route)
            routes.append(route)
    return routes or ["错误"]


def _collect_usage(total: dict[str, Any], usage: dict[str, Any]) -> None:
    for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
        value = usage.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            total[key] = total.get(key, 0) + value

    price = usage.get("total_price")
    currency = usage.get("currency")
    if price is None or not currency:
        return
    try:
        amount = Decimal(str(price))
    except InvalidOperation:
        return
    current_currency = total.get("currency")
    if current_currency not in (None, currency):
        total.pop("total_price", None)
        total.pop("currency", None)
        total["_mixed_currency"] = True
        return
    if total.get("_mixed_currency"):
        return
    total["currency"] = currency
    total["total_price"] = str(Decimal(total.get("total_price", "0")) + amount)


def _clean_usage(usage: dict[str, Any], elapsed: float) -> dict[str, Any]:
    usage.pop("_mixed_currency", None)
    usage["latency"] = round(elapsed, 3)
    if "total_tokens" not in usage:
        usage["total_tokens"] = usage.get("prompt_tokens", 0) + usage.get(
            "completion_tokens", 0
        )
    return usage


def _clean_resources(resources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for position, resource in enumerate(resources, start=1):
        item = dict(resource)
        for key in ("id", "message_id"):
            item.pop(key, None)
        item["position"] = position
        cleaned.append(item)
    return cleaned


async def _produce(
    request: ChatMessageRequest,
    ids: dict[str, str],
    queue: asyncio.Queue[tuple[bytes, bool] | object],
    client: httpx.AsyncClient,
) -> None:
    started = time.monotonic()
    task = ActiveTask(user=request.user)
    resources: list[dict[str, Any]] = []
    usage: dict[str, Any] = {}
    finalized = False
    async with state.lock:
        state.tasks[ids["task_id"]] = task

    async def emit(payload: dict[str, Any], *, paced: bool = False) -> None:
        if (
            paced
            and GATEWAY_CONFIG.get("output_pacing_enabled", True)
            and isinstance(payload.get("answer"), str)
        ):
            for unit in _text_units(payload["answer"]):
                await queue.put((encode_data(_message(ids, unit)), True))
            return
        await queue.put((encode_data(payload), False))

    async def finish(status: str) -> None:
        nonlocal finalized
        await emit(
            {
                **_base_event("message_end", ids),
                "metadata": {
                    "usage": _clean_usage(usage, time.monotonic() - started),
                    "retriever_resources": _clean_resources(resources),
                },
            }
        )
        await emit(
            {
                **_base_event("workflow_finished", ids),
                "workflow_run_id": ids["workflow_run_id"],
                "data": {
                    "id": ids["workflow_run_id"],
                    "status": status,
                    "elapsed_time": round(time.monotonic() - started, 3),
                },
            }
        )
        finalized = True

    try:
        await emit(
            {
                **_base_event("workflow_started", ids),
                "workflow_run_id": ids["workflow_run_id"],
                "data": {"id": ids["workflow_run_id"], "status": "running"},
            }
        )

        llm = OpenAICompatibleClient(client)
        raw_routes = await llm.classify(
            request.query, INTENT_CONFIG, list(WORKFLOW_CONFIGS)
        )
        routes = _safe_routes(raw_routes)
        logger.info("intent_result task_id=%s routes=%s", ids["task_id"], routes)

        if routes == ["错误"]:
            await emit(_message(ids, "意图识别服务暂时不可用，请稍后重试。"))
        elif routes == ["闲聊"]:
            try:
                async for text in llm.stream_chat(request.query, CHAT_CONFIG):
                    if task.cancelled.is_set():
                        break
                    await emit(_message(ids, text), paced=True)
            except (httpx.HTTPError, ValueError, json.JSONDecodeError) as exc:
                logger.warning("casual_chat_failed task_id=%s error=%s", ids["task_id"], exc)
                await emit(_message(ids, "闲聊服务暂时不可用，请稍后重试。"))
        else:
            dify = DifyClient(client)
            for index, workflow in enumerate(routes):
                if task.cancelled.is_set():
                    break
                config = WORKFLOW_CONFIGS[workflow]
                title = str(config.get("title") or workflow)
                heading = f"### {title}\n\n" if index == 0 else f"\n\n### {title}\n\n"
                await emit(_message(ids, heading))

                had_text = False
                child_resources: list[dict[str, Any]] | None = None
                child_usage: dict[str, Any] = {}
                child_conversation = await state.get_child_conversation(
                    ids["conversation_id"], workflow
                )
                try:
                    async for child_event in dify.stream_chatflow(
                        request, config, child_conversation
                    ):
                        if task.cancelled.is_set():
                            break
                        payload = child_event.payload
                        returned_conversation = payload.get("conversation_id")
                        if isinstance(returned_conversation, str):
                            await state.save_child_conversation(
                                ids["conversation_id"], workflow, returned_conversation
                            )
                        child_task_id = payload.get("task_id")
                        if isinstance(child_task_id, str):
                            await state.set_child_task(ids["task_id"], workflow, child_task_id)

                        if child_event.event == "message":
                            answer = payload.get("answer")
                            if isinstance(answer, str) and answer:
                                had_text = True
                                await emit(_message(ids, answer), paced=True)
                        elif child_event.event == "message_end":
                            metadata = payload.get("metadata") or {}
                            found = metadata.get("retriever_resources") or []
                            child_resources = [x for x in found if isinstance(x, dict)]
                            if isinstance(metadata.get("usage"), dict):
                                child_usage = metadata["usage"]
                        elif child_event.event == "error":
                            raise RuntimeError(str(payload.get("message") or "上游返回错误"))

                    if child_resources is not None:
                        resources.extend(child_resources)
                        _collect_usage(usage, child_usage)
                except (httpx.HTTPError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
                    logger.warning(
                        "child_failed task_id=%s workflow=%s partial=%s error_type=%s error=%r",
                        ids["task_id"],
                        workflow,
                        had_text,
                        type(exc).__name__,
                        exc,
                    )
                    failure = (
                        "\n\n> 本模块回答未完整生成，请稍后重试。"
                        if had_text
                        else "该服务暂时无法响应，请稍后重试。"
                    )
                    await emit(_message(ids, failure))
                finally:
                    await state.set_child_task(ids["task_id"], workflow, None)

        status = "stopped" if task.cancelled.is_set() else "succeeded"
        await finish(status)
    except asyncio.CancelledError:
        task.cancelled.set()
        raise
    except Exception:
        logger.exception("unhandled_orchestration_error task_id=%s", ids["task_id"])
        await emit(_message(ids, "网关处理请求时发生错误，请稍后重试。"))
        if not finalized:
            await finish("failed")
    finally:
        async with state.lock:
            state.tasks.pop(ids["task_id"], None)
        current = asyncio.current_task()
        if current is None or not current.cancelling():
            await queue.put(_END)


async def stream_response(request: ChatMessageRequest) -> AsyncIterator[bytes]:
    ids = _public_ids(request.conversation_id)
    queue: asyncio.Queue[tuple[bytes, bool] | object] = asyncio.Queue(
        maxsize=max(1, int(GATEWAY_CONFIG.get("output_buffer_units", _OUTPUT_QUEUE_MAXSIZE)))
    )
    timeout = httpx.Timeout(
        connect=float(GATEWAY_CONFIG.get("connect_timeout_seconds", 10)),
        read=float(GATEWAY_CONFIG.get("read_timeout_seconds", 300)),
        write=30,
        pool=10,
    )
    async with httpx.AsyncClient(timeout=timeout) as client:
        producer = asyncio.create_task(_produce(request, ids, queue, client))
        try:
            yield encode_ping()
            heartbeat = max(
                0.1, float(GATEWAY_CONFIG.get("heartbeat_interval_seconds", 15))
            )
            pacing_enabled = bool(GATEWAY_CONFIG.get("output_pacing_enabled", True))
            pacing_mode = str(GATEWAY_CONFIG.get("output_pacing_mode", "adaptive"))
            base_rate = max(0.1, float(GATEWAY_CONFIG.get("output_tokens_per_second", 7)))
            max_rate = max(base_rate, float(GATEWAY_CONFIG.get("output_max_tokens_per_second", 28)))
            backlog_threshold = max(1, int(GATEWAY_CONFIG.get("output_backlog_threshold", 35)))
            next_token_at = time.monotonic()
            started = next_token_at
            sent_units = 0
            max_backlog = 0
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=heartbeat)
                except TimeoutError:
                    yield encode_ping()
                    continue
                if item is _END:
                    break
                frame, paced = item
                if paced and pacing_enabled:
                    backlog = queue.qsize()
                    max_backlog = max(max_backlog, backlog)
                    rate = base_rate
                    if pacing_mode == "adaptive" and backlog > backlog_threshold:
                        rate = min(max_rate, base_rate * (1 + backlog / backlog_threshold))
                    delay = next_token_at - time.monotonic()
                    while delay > 0:
                        if delay >= heartbeat:
                            await asyncio.sleep(heartbeat)
                            yield encode_ping()
                        else:
                            await asyncio.sleep(delay)
                        delay = next_token_at - time.monotonic()
                    now = time.monotonic()
                    next_token_at = max(next_token_at, now) + 1 / rate
                    sent_units += 1
                yield frame
            if sent_units:
                elapsed = max(time.monotonic() - started, 0.001)
                logger.info(
                    "stream_pacing task_id=%s mode=%s units=%d avg_units_per_second=%.2f max_backlog=%d",
                    ids["task_id"], pacing_mode if pacing_enabled else "disabled",
                    sent_units, sent_units / elapsed, max_backlog,
                )
        finally:
            if not producer.done():
                producer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await producer


async def stop_task(public_task_id: str, user: str) -> bool:
    async with state.lock:
        active = state.tasks.get(public_task_id)
        if not active or active.user != user:
            return False
        active.cancelled.set()
        workflow = active.current_workflow
        child_task_id = active.current_child_task_id

    if workflow and child_task_id and workflow in WORKFLOW_CONFIGS:
        timeout = httpx.Timeout(10)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                await DifyClient(client).stop(
                    WORKFLOW_CONFIGS[workflow], child_task_id, user
                )
        except httpx.HTTPError as exc:
            logger.warning(
                "child_stop_failed task_id=%s workflow=%s error=%s",
                public_task_id,
                workflow,
                exc,
            )
    return True
