"""FastAPI service: live seismic entropy, a WebSocket feed for the visual
wall, and a small public REST API (ANU QRNG style) for pulling seeds.

Entropy flows: SeedLink background thread -> EntropyPool -> two sinks:
  - a broadcast to every connected WebSocket (the visual)
  - a thread-safe FIFO that /api/random pops from (numbers served over the
    API are consumed once and never repeated to another caller)
"""

import asyncio
import collections
import logging
import pathlib
import queue
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from entropy import EntropyPool
from seedlink_source import SeismicFeed, STATIONS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("server")

FRONTEND_DIR = pathlib.Path(__file__).resolve().parent.parent / "frontend"

app = FastAPI(title="Seismic Entropy")

pool = EntropyPool()
feed = SeismicFeed(on_samples=pool.ingest)

api_queue = queue.Queue()  # numbers waiting to be served via /api/random, FIFO
recent_events = collections.deque(maxlen=200)  # for late-joining WS clients

_ws_clients = set()
_main_loop = None  # captured at startup so the SeedLink thread can hand events to asyncio


def _on_entropy_event(event):
    recent_events.append(event)
    api_queue.put(event)
    if _main_loop is not None:
        _main_loop.call_soon_threadsafe(
            lambda: asyncio.ensure_future(_broadcast(event))
        )


async def _broadcast(event):
    dead = []
    for ws in list(_ws_clients):
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


pool.add_sink(_on_entropy_event)


@app.on_event("startup")
async def startup():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    feed.start()
    log.info("seedlink feed starting, stations: %s", [s[3] for s in STATIONS])


@app.get("/")
async def index():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/api/status")
async def status():
    return {
        "connected": feed.connected,
        "packets_received": feed.packets_received,
        "last_packet_age_seconds": (
            round(time.time() - feed.last_packet_ts, 1) if feed.last_packet_ts else None
        ),
        "stations": [
            {"id": f"{n}.{s}", "label": label} for n, s, _ch, label in STATIONS
        ],
        "numbers_queued_for_api": api_queue.qsize(),
    }


@app.get("/api/random")
async def api_random(n: int = 10, timeout: float = 5.0):
    """Pull up to `n` fresh 32-bit integers (0-4294967295), each served
    exactly once. Waits up to `timeout` seconds total for enough entropy to
    accumulate if the live feed hasn't produced enough yet -- packets
    arrive in bursts, not an even trickle, so a short wait is normal."""
    n = max(1, min(n, 256))
    deadline = time.time() + timeout
    results = []
    while len(results) < n and time.time() < deadline:
        remaining = deadline - time.time()
        try:
            event = api_queue.get(timeout=max(0.05, remaining))
        except queue.Empty:
            break
        results.append(
            {
                "value": event["value"],
                "digits": event["digits"],
                "station": event["station_label"],
                "station_id": event["station_id"],
                "ts": event["ts"],
            }
        )
    return JSONResponse(
        {
            "success": True,
            "requested": n,
            "returned": len(results),
            "data": results,
            "source": "live seismic sensor noise (SHA-256 of least-significant "
            "bits, IRIS/EarthScope SeedLink)",
        }
    )


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        for event in list(recent_events)[-40:]:
            await websocket.send_json(event)
        while True:
            await websocket.receive_text()  # just keep the connection open
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
