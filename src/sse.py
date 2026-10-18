import json
from collections.abc import AsyncIterator
from typing import Any


def encode_data(payload: dict[str, Any]) -> bytes:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"data: {body}\n\n".encode()


def encode_ping() -> bytes:
    return b"event: ping\n\n"


async def parse_sse(lines: AsyncIterator[str]) -> AsyncIterator[tuple[str | None, str]]:
    """Parse SSE frames from decoded lines, including multi-line data fields."""
    event_name: str | None = None
    data_lines: list[str] = []

    async for raw_line in lines:
        line = raw_line.rstrip("\r")
        if line == "":
            if data_lines or event_name:
                yield event_name, "\n".join(data_lines)
            event_name = None
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        field, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_name = value
        elif field == "data":
            data_lines.append(value)

    if data_lines or event_name:
        yield event_name, "\n".join(data_lines)
