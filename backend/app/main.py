import asyncio
import json

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import connect, disconnect
from app.repository import database_ping, latest_trends, redis_ping, trend_history
from app.schemas import HealthResponse

app = FastAPI(title="Real-Time Twitter Trends Analytics API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup() -> None:
    await connect()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await disconnect()


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    db_ok = await database_ping()
    redis_ok = await redis_ping()
    return HealthResponse(
        status="ok" if db_ok and redis_ok else "degraded",
        database="ok" if db_ok else "error",
        redis="ok" if redis_ok else "error",
    )


@app.get("/api/trends/latest")
async def get_latest_trends(limit: int = Query(default=20, ge=1, le=100)) -> list[dict]:
    return await latest_trends(limit=limit)


@app.get("/api/trends/history/{keyword}")
async def get_trend_history(keyword: str, hours: int = Query(default=2, ge=1, le=72)) -> list[dict]:
    return await trend_history(keyword=keyword, hours=hours)


@app.websocket("/ws/trends")
async def websocket_trends(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            payload = await latest_trends(limit=20)
            await websocket.send_text(json.dumps(payload))
            await asyncio.sleep(settings.api_refresh_seconds)
    except WebSocketDisconnect:
        return
