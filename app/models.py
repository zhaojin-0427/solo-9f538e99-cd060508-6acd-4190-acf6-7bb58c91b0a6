"""Pydantic request/response models."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class FragmentIn(BaseModel):
    station: str = Field(min_length=1, max_length=64)
    minute_utc: datetime
    fragment_no: int = Field(ge=0, le=9999)
    revision: int = Field(ge=1)
    start_sample: int = Field(ge=0, le=59)
    samples: list[int] = Field(min_length=1, max_length=60)

    @field_validator("station")
    @classmethod
    def _station_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("station must not be blank")
        return v

    @field_validator("minute_utc")
    @classmethod
    def _whole_minute(cls, v: datetime) -> datetime:
        # Reject sub-minute precision: every fragment belongs to a whole minute.
        if v.second != 0 or v.microsecond != 0:
            raise ValueError("minute_utc must be a whole minute (seconds=0)")
        return v


class FragmentOut(BaseModel):
    station: str
    minute_utc: datetime
    fragment_no: int
    revision: int
    start_sample: int
    samples: list[int]
    content_hash: str


class ConflictOut(BaseModel):
    position: int
    fragment_nos: list[int]
    values: list[int]


class AssemblyOut(BaseModel):
    station: str
    minute_utc: datetime
    complete: bool
    coverage: int
    gaps: list[int]
    conflicts: list[ConflictOut]
    fragments: list[FragmentOut]


class PublicationItemOut(BaseModel):
    fragment_no: int
    revision: int
    start_sample: int
    samples: list[int]
    content_hash: str


class PublicationOut(BaseModel):
    id: int
    station: str
    minute_utc: datetime
    wave: list[int]
    stale: bool
    fragment_set_hash: str
    created_at: str
    items: list[PublicationItemOut]


# --------------------------------------------------------------------------- #
# Single-station data feed
# --------------------------------------------------------------------------- #
class SubscriptionOut(BaseModel):
    subscription_id: int
    station: str
    # Size of the immutable starting point (number of pages worth of minutes
    # is governed by the page size used when reading the feed).
    snapshot_size: int
    created_at: str
    # Cursor to read the first page of the frozen starting point.
    next_cursor: str


class FeedEventOut(BaseModel):
    # Log offset; unique inside one subscription and monotonically increasing.
    seq: int
    station: str
    minute_utc: datetime
    # "upsert" delivers (or replaces) a minute's waveform; "withdraw" recalls
    # a previously delivered publication id.
    event_type: str
    publication_id: int | None = None
    # Populated for upsert events; absent on withdraw.
    publication: PublicationOut | None = None


class FeedPageOut(BaseModel):
    subscription_id: int
    station: str
    # "snapshot" pages deliver the frozen starting point; "live" pages
    # deliver withdraw/re-publish changes that happened afterwards.
    phase: str
    events: list[FeedEventOut]
    next_cursor: str | None = None
    # True while the starting point is still being paged through.
    has_more: bool
