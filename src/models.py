from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ChatMessageRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    inputs: dict[str, Any] = Field(default_factory=dict)
    query: str = Field(min_length=1)
    response_mode: Literal["streaming"]
    conversation_id: str = ""
    user: str = Field(min_length=1)
    files: list[dict[str, Any]] = Field(default_factory=list)


class StopRequest(BaseModel):
    user: str = Field(min_length=1)


class ChildEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    event: str
    payload: dict[str, Any]
