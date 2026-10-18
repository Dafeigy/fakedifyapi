import logging
import secrets

from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.responses import JSONResponse, StreamingResponse

from .models import ChatMessageRequest, StopRequest
from .orchestrator import stop_task, stream_response
from .settings import GATEWAY_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(title="FakeDifyAPI", version="0.1.0")


async def verify_api_key(authorization: str | None = Header(default=None)) -> None:
    expected = str(GATEWAY_CONFIG.get("api_key") or "")
    if not expected:
        return
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not secrets.compare_digest(token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/chat-messages", dependencies=[Depends(verify_api_key)])
async def chat_messages(body: ChatMessageRequest) -> StreamingResponse:
    return StreamingResponse(
        stream_response(body),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.post(
    "/v1/chat-messages/{task_id}/stop",
    dependencies=[Depends(verify_api_key)],
)
async def stop_chat_message(task_id: str, body: StopRequest) -> JSONResponse:
    stopped = await stop_task(task_id, body.user)
    if not stopped:
        raise HTTPException(status_code=404, detail="Task not found")
    return JSONResponse({"result": "success"})
