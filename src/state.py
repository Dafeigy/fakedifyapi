import asyncio
from contextlib import closing
import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, TypeVar

from .settings import GATEWAY_CONFIG

_T = TypeVar("_T")
_LOCK_LEASE_SECONDS = 60
_LOCK_RENEW_INTERVAL_SECONDS = 10


def _database_path() -> Path:
    configured = Path(
        str(GATEWAY_CONFIG.get("state_db") or "data/gateway_state.sqlite3")
    )
    if configured.is_absolute():
        return configured
    return Path(__file__).resolve().parent.parent / configured


@dataclass
class ActiveTask:
    user: str
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    current_workflow: str | None = None
    current_child_task_id: str | None = None


class MemoryState:
    """Runtime task state plus SQLite-persisted cross-worker conversation state."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or _database_path()
        self.tasks: dict[str, ActiveTask] = {}
        self.lock = asyncio.Lock()
        self._initialize_database()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _transaction(self, operation: Callable[[sqlite3.Connection], _T]) -> _T:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = operation(connection)
                connection.commit()
                return result
            except BaseException:
                connection.rollback()
                raise

    def _initialize_database(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS requests (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL UNIQUE,
                    user TEXT NOT NULL,
                    public_id TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    query TEXT NOT NULL,
                    routes_json TEXT NOT NULL DEFAULT '[]',
                    elapsed_seconds REAL,
                    status TEXT NOT NULL,
                    success INTEGER
                );
                CREATE INDEX IF NOT EXISTS requests_session_order
                    ON requests(user, public_id, seq, status);
                CREATE TABLE IF NOT EXISTS conversations (
                    user TEXT NOT NULL,
                    public_id TEXT NOT NULL,
                    workflow TEXT NOT NULL,
                    child_conversation_id TEXT NOT NULL,
                    PRIMARY KEY(user, public_id, workflow)
                );
                CREATE TABLE IF NOT EXISTS child_runs (
                    request_id TEXT NOT NULL,
                    workflow TEXT NOT NULL,
                    child_conversation_id TEXT,
                    status TEXT NOT NULL,
                    elapsed_seconds REAL,
                    answer TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(request_id, workflow),
                    FOREIGN KEY(request_id) REFERENCES requests(request_id)
                );
                CREATE TABLE IF NOT EXISTS conversation_locks (
                    user TEXT NOT NULL,
                    public_id TEXT NOT NULL,
                    owner_request_id TEXT,
                    lease_until REAL,
                    PRIMARY KEY(user, public_id)
                );
                CREATE TABLE IF NOT EXISTS active_tasks (
                    task_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    user TEXT NOT NULL,
                    workflow TEXT,
                    child_task_id TEXT,
                    stop_requested INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
        self._import_legacy_json()

    def _import_legacy_json(self) -> None:
        configured_legacy_path = GATEWAY_CONFIG.get("state_file")
        legacy_path = (
            Path(str(configured_legacy_path))
            if configured_legacy_path
            else self.path.with_name("gateway_state.json")
        )
        if not legacy_path.is_absolute():
            legacy_path = Path(__file__).resolve().parent.parent / legacy_path
        if not legacy_path.exists():
            return
        try:
            legacy = json.loads(legacy_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Unable to read legacy state from {legacy_path}: {exc}") from exc
        if not isinstance(legacy, dict) or legacy.get("version") != 1:
            raise RuntimeError(f"Unsupported legacy state format in {legacy_path}")

        def import_state(connection: sqlite3.Connection) -> None:
            imported = connection.execute(
                "SELECT value FROM metadata WHERE key = 'legacy_json_imported'"
            ).fetchone()
            if imported:
                return

            requests = [item for item in legacy.get("requests", []) if isinstance(item, dict)]
            users_by_public_id: dict[str, set[str]] = {}
            for record in requests:
                public_id = record.get("conversation_id")
                user = record.get("user")
                if isinstance(public_id, str) and isinstance(user, str):
                    users_by_public_id.setdefault(public_id, set()).add(user)

            for record in requests:
                request_id = record.get("request_id")
                public_id = record.get("conversation_id")
                user = record.get("user")
                if not all(isinstance(value, str) and value for value in (request_id, public_id, user)):
                    continue
                child_records = record.get("sub_workflows", {})
                if not isinstance(child_records, dict):
                    child_records = {}
                request_status = str(record.get("status") or "interrupted")
                if request_status == "in_progress":
                    request_status = "interrupted"
                connection.execute(
                    """INSERT OR IGNORE INTO requests
                    (request_id,user,public_id,requested_at,query,routes_json,elapsed_seconds,status,success)
                    VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        request_id,
                        user,
                        public_id,
                        str(record.get("requested_at") or ""),
                        str(record.get("query") or ""),
                        json.dumps(record.get("routes") or [], ensure_ascii=False),
                        record.get("elapsed_seconds"),
                        request_status,
                        int(bool(record.get("success")))
                        if record.get("success") is not None
                        else None,
                    ),
                )
                for workflow, child in child_records.items():
                    if not isinstance(child, dict):
                        continue
                    child_status = str(child.get("status") or "interrupted")
                    if child_status == "in_progress":
                        child_status = "interrupted"
                    connection.execute(
                        """INSERT OR IGNORE INTO child_runs
                        (request_id,workflow,child_conversation_id,status,elapsed_seconds,answer)
                        VALUES(?,?,?,?,?,?)""",
                        (
                            request_id,
                            str(workflow),
                            child.get("conversation_id"),
                            child_status,
                            child.get("elapsed_seconds"),
                            str(child.get("answer") or ""),
                        ),
                    )

            for public_id, workflows in (legacy.get("conversations") or {}).items():
                users = users_by_public_id.get(str(public_id), set())
                if len(users) != 1 or not isinstance(workflows, dict):
                    continue
                user = next(iter(users))
                for workflow, child_id in workflows.items():
                    if isinstance(child_id, str) and child_id:
                        connection.execute(
                            """INSERT OR IGNORE INTO conversations
                            (user,public_id,workflow,child_conversation_id) VALUES(?,?,?,?)""",
                            (user, str(public_id), str(workflow), child_id),
                        )

            connection.execute(
                "INSERT INTO metadata(key,value) VALUES('legacy_json_imported','1')"
            )

        self._transaction(import_state)

    async def begin_request(
        self,
        *,
        request_id: str,
        public_id: str,
        user: str,
        query: str,
        requested_at: str,
    ) -> None:
        await asyncio.to_thread(
            self._transaction,
            lambda connection: connection.execute(
                """INSERT INTO requests
                (request_id,user,public_id,requested_at,query,status,success)
                VALUES(?,?,?,?,?,'queued',NULL)""",
                (request_id, user, public_id, requested_at, query),
            ),
        )

    async def acquire_conversation_lock(
        self, user: str, public_id: str, request_id: str, task_id: str
    ) -> bool:
        while True:
            if await self.is_stop_requested(task_id):
                return False
            acquired = await asyncio.to_thread(
                self._try_acquire_conversation_lock, user, public_id, request_id
            )
            if acquired:
                return True
            await asyncio.sleep(0.25)

    def _try_acquire_conversation_lock(
        self, user: str, public_id: str, request_id: str
    ) -> bool:
        now = time.time()

        def acquire(connection: sqlite3.Connection) -> bool:
            connection.execute(
                """INSERT OR IGNORE INTO conversation_locks
                (user,public_id,owner_request_id,lease_until) VALUES(?,?,NULL,NULL)""",
                (user, public_id),
            )
            lock = connection.execute(
                """SELECT owner_request_id,lease_until FROM conversation_locks
                WHERE user=? AND public_id=?""",
                (user, public_id),
            ).fetchone()
            owner = lock["owner_request_id"]
            lease_until = lock["lease_until"]
            if owner == request_id:
                connection.execute(
                    "UPDATE conversation_locks SET lease_until=? WHERE user=? AND public_id=?",
                    (now + _LOCK_LEASE_SECONDS, user, public_id),
                )
                return True
            if owner is not None and lease_until is not None and lease_until > now:
                return False
            if owner:
                connection.execute(
                    "DELETE FROM active_tasks WHERE request_id=?", (owner,)
                )
                connection.execute(
                    """UPDATE child_runs SET status='interrupted'
                    WHERE request_id=? AND status='in_progress'""",
                    (owner,),
                )
                connection.execute(
                    """UPDATE requests SET status='interrupted',
                    success=EXISTS(SELECT 1 FROM child_runs
                        WHERE request_id=? AND status='succeeded')
                    WHERE request_id=? AND status='in_progress'""",
                    (owner, owner),
                )

            first_queued = connection.execute(
                """SELECT request_id FROM requests
                WHERE user=? AND public_id=? AND status='queued'
                ORDER BY seq LIMIT 1""",
                (user, public_id),
            ).fetchone()
            if first_queued is None or first_queued["request_id"] != request_id:
                return False
            connection.execute(
                """UPDATE conversation_locks
                SET owner_request_id=?,lease_until=? WHERE user=? AND public_id=?""",
                (request_id, now + _LOCK_LEASE_SECONDS, user, public_id),
            )
            connection.execute(
                "UPDATE requests SET status='in_progress' WHERE request_id=? AND status='queued'",
                (request_id,),
            )
            return True

        return self._transaction(acquire)

    async def renew_conversation_lock(
        self, user: str, public_id: str, request_id: str
    ) -> bool:
        now = time.time()
        return await asyncio.to_thread(
            self._transaction,
            lambda connection: connection.execute(
                """UPDATE conversation_locks SET lease_until=?
                WHERE user=? AND public_id=? AND owner_request_id=?""",
                (now + _LOCK_LEASE_SECONDS, user, public_id, request_id),
            ).rowcount
            == 1,
        )

    async def release_conversation_lock(
        self, user: str, public_id: str, request_id: str
    ) -> None:
        await asyncio.to_thread(
            self._transaction,
            lambda connection: connection.execute(
                """UPDATE conversation_locks SET owner_request_id=NULL,lease_until=NULL
                WHERE user=? AND public_id=? AND owner_request_id=?""",
                (user, public_id, request_id),
            ),
        )

    async def set_routes(self, request_id: str, routes: list[str]) -> None:
        await asyncio.to_thread(
            self._transaction,
            lambda connection: connection.execute(
                "UPDATE requests SET routes_json=? WHERE request_id=? AND status='in_progress'",
                (json.dumps(routes, ensure_ascii=False), request_id),
            ),
        )

    async def get_child_conversation(self, user: str, public_id: str, workflow: str) -> str:
        def query() -> str:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """SELECT child_conversation_id FROM conversations
                    WHERE user=? AND public_id=? AND workflow=?""",
                    (user, public_id, workflow),
                ).fetchone()
                return str(row["child_conversation_id"]) if row else ""

        return await asyncio.to_thread(query)

    async def save_child_conversation(
        self,
        user: str,
        public_id: str,
        workflow: str,
        child_id: str,
        request_id: str,
    ) -> None:
        if not child_id:
            return

        def save(connection: sqlite3.Connection) -> None:
            active = connection.execute(
                "SELECT 1 FROM requests WHERE request_id=? AND status='in_progress'",
                (request_id,),
            ).fetchone()
            if active is None:
                return
            connection.execute(
                """INSERT INTO conversations(user,public_id,workflow,child_conversation_id)
                VALUES(?,?,?,?) ON CONFLICT(user,public_id,workflow) DO UPDATE SET
                child_conversation_id=excluded.child_conversation_id""",
                (user, public_id, workflow, child_id),
            )
            connection.execute(
                """INSERT INTO child_runs
                (request_id,workflow,child_conversation_id,status,answer)
                VALUES(?,?,?,'in_progress','')
                ON CONFLICT(request_id,workflow) DO UPDATE SET
                child_conversation_id=excluded.child_conversation_id""",
                (request_id, workflow, child_id),
            )

        await asyncio.to_thread(self._transaction, save)

    async def record_child_result(
        self,
        request_id: str,
        workflow: str,
        *,
        status: str,
        elapsed_seconds: float,
        answer: str,
    ) -> None:
        def save(connection: sqlite3.Connection) -> None:
            active = connection.execute(
                "SELECT 1 FROM requests WHERE request_id=? AND status='in_progress'",
                (request_id,),
            ).fetchone()
            if active is None:
                return
            connection.execute(
                """INSERT INTO child_runs
                (request_id,workflow,status,elapsed_seconds,answer)
                VALUES(?,?,?,?,?)
                ON CONFLICT(request_id,workflow) DO UPDATE SET
                status=excluded.status,elapsed_seconds=excluded.elapsed_seconds,
                answer=excluded.answer""",
                (request_id, workflow, status, round(elapsed_seconds, 3), answer),
            )

        await asyncio.to_thread(self._transaction, save)

    async def finish_request(
        self,
        request_id: str,
        *,
        elapsed_seconds: float,
        success: bool,
        status: str,
    ) -> None:
        def finish(connection: sqlite3.Connection) -> None:
            current = connection.execute(
                "SELECT status FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if current is None or current["status"] == "interrupted":
                return
            connection.execute(
                """UPDATE requests SET elapsed_seconds=?,success=?,status=?
                WHERE request_id=?""",
                (round(elapsed_seconds, 3), int(success), status, request_id),
            )

        await asyncio.to_thread(self._transaction, finish)

    async def request_has_successful_child(self, request_id: str) -> bool:
        def query() -> bool:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    "SELECT 1 FROM child_runs WHERE request_id=? AND status='succeeded' LIMIT 1",
                    (request_id,),
                ).fetchone()
                return row is not None

        return await asyncio.to_thread(query)

    async def get_recent_context(
        self, user: str, public_id: str, limit: int = 3
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []

        def query() -> list[dict[str, Any]]:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """SELECT request_id,query FROM requests
                    WHERE user=? AND public_id=? AND status IN ('succeeded','failed','stopped')
                    ORDER BY seq DESC LIMIT ?""",
                    (user, public_id, limit),
                ).fetchall()
                context: list[dict[str, Any]] = []
                for row in reversed(rows):
                    children = connection.execute(
                        """SELECT workflow,answer FROM child_runs
                        WHERE request_id=? AND status='succeeded' AND answer!=''
                        ORDER BY rowid""",
                        (row["request_id"],),
                    ).fetchall()
                    context.append(
                        {
                            "query": row["query"],
                            "answers": [
                                {"workflow": child["workflow"], "answer": child["answer"]}
                                for child in children
                            ],
                        }
                    )
                return context

        return await asyncio.to_thread(query)

    async def set_child_task(
        self, public_task_id: str, workflow: str, child_task_id: str | None
    ) -> None:
        async with self.lock:
            task = self.tasks.get(public_task_id)
            if task:
                task.current_workflow = workflow
                task.current_child_task_id = child_task_id
        await asyncio.to_thread(
            self._transaction,
            lambda connection: connection.execute(
                """UPDATE active_tasks SET workflow=?,child_task_id=?
                WHERE task_id=?""",
                (workflow if child_task_id else None, child_task_id, public_task_id),
            ),
        )

    async def register_task(self, task_id: str, user: str, request_id: str) -> None:
        await asyncio.to_thread(
            self._transaction,
            lambda connection: connection.execute(
                "INSERT INTO active_tasks(task_id,request_id,user) VALUES(?,?,?)",
                (task_id, request_id, user),
            ),
        )

    async def unregister_task(self, task_id: str) -> None:
        await asyncio.to_thread(
            self._transaction,
            lambda connection: connection.execute(
                "DELETE FROM active_tasks WHERE task_id=?", (task_id,)
            ),
        )

    async def request_stop(
        self, task_id: str, user: str
    ) -> tuple[bool, str | None, str | None]:
        def stop(connection: sqlite3.Connection) -> tuple[bool, str | None, str | None]:
            cursor = connection.execute(
                """UPDATE active_tasks SET stop_requested=1
                WHERE task_id=? AND user=?""",
                (task_id, user),
            )
            if cursor.rowcount != 1:
                return False, None, None
            row = connection.execute(
                "SELECT workflow,child_task_id FROM active_tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            return True, row["workflow"], row["child_task_id"]

        return await asyncio.to_thread(self._transaction, stop)

    async def is_stop_requested(self, task_id: str) -> bool:
        def query() -> bool:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    "SELECT stop_requested FROM active_tasks WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                return bool(row and row["stop_requested"])

        return await asyncio.to_thread(query)


state = MemoryState()
