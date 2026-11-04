import asyncio
import contextlib
import json
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
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
_OUTPUT_MAX_DELAY_SECONDS = 2.0


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


def _clean_usage(usage: dict[str, Any], elapsed_ms: int) -> dict[str, Any]:
    usage.pop("_mixed_currency", None)
    usage["latency"] = elapsed_ms
    if "total_tokens" not in usage:
        usage["total_tokens"] = usage.get("prompt_tokens", 0) + usage.get(
            "completion_tokens", 0
        )
    return usage


def _elapsed_ms(started: float) -> int:
    return round((time.monotonic() - started) * 1000)


def _clean_resources(resources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for position, resource in enumerate(resources, start=1):
        item = dict(resource)
        for key in ("id", "message_id"):
            item.pop(key, None)
        item["position"] = position
        cleaned.append(item)
    return cleaned


async def _maintain_conversation_lock(
    request: ChatMessageRequest,
    public_id: str,
    request_id: str,
    task_id: str,
    task: ActiveTask,
    producer: asyncio.Task[Any],
) -> None:
    last_renewal = time.monotonic()
    while True:
        await asyncio.sleep(0.5)
        try:
            if await state.is_stop_requested(task_id):
                task.cancelled.set()
                return
        except Exception:
            logger.exception("stop_state_poll_failed task_id=%s", task_id)
        try:
            if time.monotonic() - last_renewal < 10:
                continue
            renewed = await state.renew_conversation_lock(request.user, public_id, request_id)
        except Exception:
            logger.exception("conversation_lock_renewal_failed request_id=%s", request_id)
            continue
        last_renewal = time.monotonic()
        if not renewed:
            task.cancelled.set()
            producer.cancel()
            return


async def _produce(
    request: ChatMessageRequest,
    ids: dict[str, str],
    queue: asyncio.Queue[tuple[bytes, bool, float] | object],
    client: httpx.AsyncClient,
) -> None:
    started = time.monotonic()
    requested_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    request_id = ids["message_id"]
    task = ActiveTask(user=request.user)
    resources: list[dict[str, Any]] = []
    usage: dict[str, Any] = {}
    finalized = False
    conversation_lock_acquired = False
    lock_heartbeat: asyncio.Task[None] | None = None
    async with state.lock:
        state.tasks[ids["task_id"]] = task
    async def emit(payload: dict[str, Any], *, paced: bool = False) -> None:
        if (
            paced
            and GATEWAY_CONFIG.get("output_pacing_enabled", True)
            and isinstance(payload.get("answer"), str)
        ):
            for unit in _text_units(payload["answer"]):
                await queue.put((encode_data(_message(ids, unit)), True, time.monotonic()))
            return
        await queue.put((encode_data(payload), False, time.monotonic()))

    async def finish(status: str) -> None:
        nonlocal finalized
        await emit(
            {
                **_base_event("message_end", ids),
                "metadata": {
                    "usage": _clean_usage(usage, _elapsed_ms(started)),
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
                    "elapsed_time": _elapsed_ms(started),
                },
            }
        )
        finalized = True

    try:
        await state.register_task(ids["task_id"], request.user, request_id)
        await state.begin_request(
            request_id=request_id,
            public_id=ids["conversation_id"],
            user=request.user,
            query=request.query,
            requested_at=requested_at,
        )
        conversation_lock_acquired = await state.acquire_conversation_lock(
            request.user,
            ids["conversation_id"],
            request_id,
            ids["task_id"],
        )
        if conversation_lock_acquired:
            producer = asyncio.current_task()
            if producer is not None:
                lock_heartbeat = asyncio.create_task(
                    _maintain_conversation_lock(
                        request,
                        ids["conversation_id"],
                        request_id,
                        ids["task_id"],
                        task,
                        producer,
                    )
                )
        await emit(
            {
                **_base_event("workflow_started", ids),
                "workflow_run_id": ids["workflow_run_id"],
                "data": {"id": ids["workflow_run_id"], "status": "running"},
            }
        )

        llm = OpenAICompatibleClient(client)
        conversation_context = await state.get_recent_context(
            request.user, ids["conversation_id"], 3
        )
        raw_routes = await llm.classify(
            request.query,
            INTENT_CONFIG,
            list(WORKFLOW_CONFIGS),
            conversation_context,
        )
        routes = _safe_routes(raw_routes)
        await state.set_routes(request_id, routes)
        logger.info("intent_result task_id=%s routes=%s", ids["task_id"], routes)

        if task.cancelled.is_set():
            pass
        elif routes == ["错误"]:
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
                child_started = time.monotonic()
                child_answer: list[str] = []
                child_status = "failed"
                saw_workflow_finished = False
                child_conversation = await state.get_child_conversation(
                    request.user, ids["conversation_id"], workflow
                )
                if child_conversation:
                    await state.save_child_conversation(
                        request.user,
                        ids["conversation_id"],
                        workflow,
                        child_conversation,
                        request_id,
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
                                request.user,
                                ids["conversation_id"],
                                workflow,
                                returned_conversation,
                                request_id,
                            )
                        child_task_id = payload.get("task_id")
                        if isinstance(child_task_id, str):
                            await state.set_child_task(ids["task_id"], workflow, child_task_id)

                        if child_event.event == "message":
                            answer = payload.get("answer")
                            if isinstance(answer, str) and answer:
                                had_text = True
                                child_answer.append(answer)
                                await emit(_message(ids, answer), paced=True)
                        elif child_event.event == "message_end":
                            metadata = payload.get("metadata") or {}
                            found = metadata.get("retriever_resources") or []
                            child_resources = [x for x in found if isinstance(x, dict)]
                            if isinstance(metadata.get("usage"), dict):
                                child_usage = metadata["usage"]
                        elif child_event.event == "error":
                            raise RuntimeError(str(payload.get("message") or "上游返回错误"))
                        elif child_event.event == "workflow_finished":
                            saw_workflow_finished = True
                            finished_data = payload.get("data")
                            child_run_status = (
                                finished_data.get("status")
                                if isinstance(finished_data, dict)
                                else None
                            )
                            if child_run_status and child_run_status != "succeeded":
                                raise RuntimeError(
                                    f"子工作流结束状态异常: {child_run_status or 'unknown'}"
                                )

                    if task.cancelled.is_set():
                        child_status = "cancelled"
                    elif not saw_workflow_finished:
                        raise RuntimeError("子工作流流结束时缺少成功的 workflow_finished 事件")
                    else:
                        child_status = "succeeded"

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
                    child_status = "cancelled" if task.cancelled.is_set() else "failed"
                    failure = (
                        "\n\n> 本模块回答未完整生成，请稍后重试。"
                        if had_text
                        else "该服务暂时无法响应，请稍后重试。"
                    )
                    await emit(_message(ids, failure))
                finally:
                    await state.record_child_result(
                        request_id,
                        workflow,
                        status=child_status,
                        elapsed_ms=_elapsed_ms(child_started),
                        answer="".join(child_answer),
                    )
                    await state.set_child_task(ids["task_id"], workflow, None)

        status = "stopped" if task.cancelled.is_set() else "succeeded"
        request_success = await state.request_has_successful_child(request_id)
        await state.finish_request(
            request_id,
            elapsed_ms=_elapsed_ms(started),
            success=request_success,
            status=status,
        )
        await finish(status)
    except asyncio.CancelledError:
        task.cancelled.set()
        await state.finish_request(
            request_id,
            elapsed_ms=_elapsed_ms(started),
            success=await state.request_has_successful_child(request_id),
            status="interrupted",
        )
        raise
    except Exception:
        logger.exception("unhandled_orchestration_error task_id=%s", ids["task_id"])
        await emit(_message(ids, "网关处理请求时发生错误，请稍后重试。"))
        await state.finish_request(
            request_id,
            elapsed_ms=_elapsed_ms(started),
            success=await state.request_has_successful_child(request_id),
            status="failed",
        )
        if not finalized:
            await finish("failed")
    finally:
        if lock_heartbeat is not None:
            lock_heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await lock_heartbeat
        if conversation_lock_acquired:
            with contextlib.suppress(Exception):
                await state.release_conversation_lock(
                    request.user, ids["conversation_id"], request_id
                )
        async with state.lock:
            state.tasks.pop(ids["task_id"], None)
        with contextlib.suppress(Exception):
            await state.unregister_task(ids["task_id"])
        current = asyncio.current_task()
        if current is None or not current.cancelling():
            await queue.put(_END)


async def stream_response(request: ChatMessageRequest) -> AsyncIterator[bytes]:
    ids = _public_ids(request.conversation_id)
    queue: asyncio.Queue[tuple[bytes, bool, float] | object] = asyncio.Queue(
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
            max_delay = max(
                0.0,
                float(GATEWAY_CONFIG.get("output_max_delay_seconds", _OUTPUT_MAX_DELAY_SECONDS)),
            )
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
                frame, paced, enqueued_at = item
                if paced and pacing_enabled:
                    backlog = queue.qsize()
                    max_backlog = max(max_backlog, backlog)
                    rate = base_rate
                    if pacing_mode == "adaptive" and backlog > backlog_threshold:
                        rate = min(max_rate, base_rate * (1 + backlog / backlog_threshold))
                    # Do not let presentation pacing block upstream reads or make
                    # terminal events wait behind an old burst of text.
                    deadline = enqueued_at + max_delay
                    now = time.monotonic()
                    delay = (
                        0.0 if backlog >= queue.maxsize - 1
                        else max(0.0, min(next_token_at, deadline) - now)
                    )
                    while delay > 0:
                        if delay >= heartbeat:
                            await asyncio.sleep(heartbeat)
                            yield encode_ping()
                        else:
                            await asyncio.sleep(delay)
                        now = time.monotonic()
                        delay = (
                            0.0 if queue.qsize() >= queue.maxsize - 1
                            else max(0.0, min(next_token_at, deadline) - now)
                        )
                    now = time.monotonic()
                    next_token_at = (
                        now if now - enqueued_at >= max_delay else max(next_token_at, now)
                    ) + 1 / rate
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
    found, workflow, child_task_id = await state.request_stop(public_task_id, user)
    if not found:
        return False
    async with state.lock:
        active = state.tasks.get(public_task_id)
        if active and active.user == user:
            active.cancelled.set()
            workflow = active.current_workflow or workflow
            child_task_id = active.current_child_task_id or child_task_id

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
