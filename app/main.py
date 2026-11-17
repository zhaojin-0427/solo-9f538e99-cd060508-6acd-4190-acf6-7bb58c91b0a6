"""FastAPI application and HTTP routes."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from . import service
from .database import init_db
from .models import (
    AssemblyOut,
    FeedPageOut,
    FragmentIn,
    FragmentOut,
    PublicationOut,
    SubscriptionOut,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Seismic Waveform Assembly API",
    version="1.1.0",
    description=(
        "Ingest out-of-order waveform fragments, assemble them into whole "
        "UTC-minute records and publish immutable minutes for phase pickers. "
        "Subscriptions expose a per-station paged feed that replays the "
        "current publications as a fixed start point, then follows minute "
        "withdrawals and re-publications over an append-only change log."
    ),
    lifespan=lifespan,
)


@app.exception_handler(service.ServiceError)
async def _service_error_handler(_: Request, exc: service.ServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": str(exc)},
    )


class UploadOut(FragmentOut):
    status: str


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/fragments", response_model=UploadOut, status_code=201)
def upload_fragment(frag: FragmentIn) -> dict[str, Any]:
    return service.upload_fragment(frag.model_dump())


@app.get("/fragments", response_model=list[FragmentOut])
def list_fragments(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> list[dict[str, Any]]:
    return service.list_fragments(station, minute_utc)


@app.get("/assembly", response_model=AssemblyOut)
def get_assembly(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> dict[str, Any]:
    return service.get_assembly(station, minute_utc)


@app.post("/publications", response_model=PublicationOut, status_code=201)
def publish_minute(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> dict[str, Any]:
    return service.publish_minute(station, minute_utc)


@app.get("/publications", response_model=list[PublicationOut])
def list_publications(
    station: str | None = Query(default=None),
    minute_utc: datetime | None = Query(default=None),
) -> list[dict[str, Any]]:
    return service.list_publications(station, minute_utc)


@app.get("/publications/latest", response_model=PublicationOut)
def get_latest_publication(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(...),
) -> JSONResponse | dict[str, Any]:
    result = service.get_latest_publication(station, minute_utc)
    if result is None:
        return JSONResponse(status_code=404, content={"detail": "publication not found"})
    return result


@app.get("/publications/{pub_id}", response_model=PublicationOut)
def get_publication(pub_id: int) -> JSONResponse | dict[str, Any]:
    result = service.get_publication(pub_id)
    if result is None:
        return JSONResponse(status_code=404, content={"detail": "publication not found"})
    return result


# --------------------------------------------------------------------------- #
# Single-station data feed
# --------------------------------------------------------------------------- #
@app.post(
    "/subscriptions",
    response_model=SubscriptionOut,
    status_code=201,
    summary="Create a single-station feed subscription",
)
def create_subscription(
    station: str = Query(
        min_length=1, description="Station whose minute waveform feed is wanted"
    ),
) -> dict[str, Any]:
    """Freeze the station's currently valid publications as the start point.

    The starting point never changes afterwards: paging it always returns the
    same publication ids and frozen waveforms.  Corrections that arrive later
    are delivered on the live tail as withdraw followed by upsert events.
    """
    return service.create_subscription(station)


@app.get(
    "/subscriptions/{subscription_id}/feed",
    response_model=FeedPageOut,
    summary="Read one page of a subscription feed",
)
def get_feed(
    subscription_id: int,
    cursor: str | None = Query(
        default=None,
        description="Opaque cursor from the previous page; omit for the first page",
    ),
    page_size: int = Query(
        default=service.DEFAULT_PAGE_SIZE,
        ge=1,
        le=service.MAX_PAGE_SIZE,
        description="Maximum events on this page",
    ),
) -> dict[str, Any]:
    """Return one deterministic page.

    Retrying the same cursor (also after a restart) returns the same page.
    Snapshot pages (``phase="snapshot"``) deliver the frozen starting point;
    once exhausted, pages switch to ``phase="live"`` and follow the append-only
    change log.  On the live tail, polling the returned cursor when
    ``has_more=false`` simply returns an empty event list with the same cursor.
    """
    return service.get_feed_page(subscription_id, cursor, page_size)
