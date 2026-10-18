import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ActiveTask:
    user: str
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    current_workflow: str | None = None
    current_child_task_id: str | None = None


class MemoryState:
    def __init__(self) -> None:
        self.conversations: dict[str, dict[str, str]] = {}
        self.tasks: dict[str, ActiveTask] = {}
        self.lock = asyncio.Lock()

    async def get_child_conversation(self, public_id: str, workflow: str) -> str:
        async with self.lock:
            return self.conversations.get(public_id, {}).get(workflow, "")

    async def save_child_conversation(
        self, public_id: str, workflow: str, child_id: str
    ) -> None:
        if not child_id:
            return
        async with self.lock:
            self.conversations.setdefault(public_id, {})[workflow] = child_id

    async def set_child_task(
        self, public_task_id: str, workflow: str, child_task_id: str | None
    ) -> None:
        async with self.lock:
            task = self.tasks.get(public_task_id)
            if task:
                task.current_workflow = workflow
                task.current_child_task_id = child_task_id


state = MemoryState()
