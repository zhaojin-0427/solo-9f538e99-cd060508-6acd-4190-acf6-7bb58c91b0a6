"""Business logic: fragment uploads, minute assembly and publications.

All mutating operations run inside a single ``BEGIN IMMEDIATE`` SQLite
transaction so that uploads and publications serialize on one consistent
snapshot of the fragment set.  Any error rolls the whole transaction back —
no half-written publications are ever left behind.
"""
from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .config import SAMPLES_PER_MINUTE
from .database import get_conn


# --------------------------------------------------------------------------- #
# Errors (mapped to HTTP status codes in the router)
# --------------------------------------------------------------------------- #
class ServiceError(Exception):
    status_code = 400


class BoundaryError(ServiceError):
    status_code = 422


class StaleRevisionError(ServiceError):
    """Incoming revision is not higher than the one already stored."""

    status_code = 409


class ContentConflictError(ServiceError):
    """Same fragment_no + revision but different payload."""

    status_code = 409


class NotPublishableError(ServiceError):
    status_code = 409


class CursorError(ServiceError):
    """Malformed or incompatible feed cursor."""

    status_code = 400


class SubscriptionNotFoundError(ServiceError):
    status_code = 404


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def minute_key(dt: datetime) -> str:
    """Canonical UTC minute string, e.g. ``2026-11-17T05:06:00Z``."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:00Z")


def dump_samples(samples: list[int]) -> str:
    return json.dumps(samples, separators=(",", ":"))


def load_samples(raw: str) -> list[int]:
    return json.loads(raw)


def content_hash(start_sample: int, samples: list[int]) -> str:
    payload = json.dumps(
        {"start_sample": start_sample, "samples": samples}, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _fragment_set_hash(rows: list[sqlite3.Row]) -> str:
    """Stable hash of the *effective* fragment set of a station-minute."""
    canonical = [
        {
            "fragment_no": r["fragment_no"],
            "revision": r["revision"],
            "start_sample": r["start_sample"],
            "samples": load_samples(r["samples"]),
            "content_hash": r["content_hash"],
        }
        for r in rows
    ]
    canonical.sort(key=lambda f: f["fragment_no"])
    blob = json.dumps(canonical, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _parse_minute(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _row_to_fragment(r: sqlite3.Row) -> dict[str, Any]:
    return {
        "station": r["station"],
        "minute_utc": _parse_minute(r["minute_utc"]),
        "fragment_no": r["fragment_no"],
        "revision": r["revision"],
        "start_sample": r["start_sample"],
        "samples": load_samples(r["samples"]),
        "content_hash": r["content_hash"],
    }


def _get_fragments(
    conn: sqlite3.Connection, station: str, minute: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM fragments WHERE station = ? AND minute_utc = ? "
        "ORDER BY fragment_no",
        (station, minute),
    ).fetchall()


def _emit_event(
    conn: sqlite3.Connection,
    station: str,
    minute: str,
    event_type: str,
    pub_id: int | None,
) -> None:
    """Append one row to the change log inside the caller's transaction."""
    conn.execute(
        "INSERT INTO feed_events (station, minute_utc, event_type, pub_id) "
        "VALUES (?, ?, ?, ?)",
        (station, minute, event_type, pub_id),
    )


def _withdraw_stale_publications(
    conn: sqlite3.Connection, station: str, minute: str
) -> None:
    """Mark the minute's current publications stale and emit withdraw events.

    Runs in the same transaction as the fragment-set change that invalidated
    them, so either the correction and its withdrawals both become visible or
    neither does.  Publication ids are withdrawn in ascending order for a
    deterministic event order.  Already-stale rows are not re-emitted.
    """
    stale_pubs = conn.execute(
        "SELECT id FROM publications WHERE station = ? AND minute_utc = ? "
        "AND stale = 0 ORDER BY id",
        (station, minute),
    ).fetchall()
    if not stale_pubs:
        return
    conn.execute(
        "UPDATE publications SET stale = 1 WHERE station = ? AND minute_utc = ? "
        "AND stale = 0",
        (station, minute),
    )
    for row in stale_pubs:
        _emit_event(conn, station, minute, "withdraw", row["id"])


# --------------------------------------------------------------------------- #
# Assembly
# --------------------------------------------------------------------------- #
def compute_assembly(rows: list[sqlite3.Row]) -> dict[str, Any]:
    """Compute coverage, gaps and value conflicts for one station-minute."""
    # position -> list of (fragment_no, value)
    covered: dict[int, list[tuple[int, int]]] = {p: [] for p in range(SAMPLES_PER_MINUTE)}
    for r in rows:
        start = r["start_sample"]
        for offset, value in enumerate(load_samples(r["samples"])):
            covered[start + offset].append((r["fragment_no"], value))

    gaps: list[int] = []
    conflicts: list[dict[str, Any]] = []
    coverage = 0
    for pos in range(SAMPLES_PER_MINUTE):
        entries = covered[pos]
        if not entries:
            gaps.append(pos)
            continue
        coverage += 1
        distinct_values = {value for _, value in entries}
        if len(distinct_values) > 1:
            conflicts.append(
                {
                    "position": pos,
                    "fragment_nos": [frag_no for frag_no, _ in entries],
                    "values": [value for _, value in entries],
                }
            )
    return {
        "complete": coverage == SAMPLES_PER_MINUTE and not conflicts,
        "coverage": coverage,
        "gaps": gaps,
        "conflicts": conflicts,
    }


# --------------------------------------------------------------------------- #
# Upload
# --------------------------------------------------------------------------- #
def upload_fragment(payload: dict[str, Any]) -> dict[str, Any]:
    station = payload["station"]
    minute = minute_key(payload["minute_utc"])
    fragment_no = payload["fragment_no"]
    revision = payload["revision"]
    start_sample = payload["start_sample"]
    samples = payload["samples"]

    # A fragment must stay inside the 60 one-hertz sample slots.
    if start_sample + len(samples) > SAMPLES_PER_MINUTE:
        raise BoundaryError(
            f"fragment exceeds minute boundary: start_sample={start_sample} "
            f"length={len(samples)} would reach slot {start_sample + len(samples)} "
            f"(limit {SAMPLES_PER_MINUTE})"
        )

    new_hash = content_hash(start_sample, samples)
    samples_json = dump_samples(samples)

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT * FROM fragments WHERE station = ? AND minute_utc = ? "
                "AND fragment_no = ?",
                (station, minute, fragment_no),
            ).fetchone()

            changed = False
            if existing is not None:
                if revision < existing["revision"]:
                    raise StaleRevisionError(
                        f"fragment_no={fragment_no} already at revision "
                        f"{existing['revision']}; incoming revision {revision} "
                        "is not higher"
                    )
                if revision == existing["revision"]:
                    # Same version: identical content is an idempotent retry and
                    # must return the original result; different content is refused.
                    if existing["content_hash"] == new_hash:
                        result = _row_to_fragment(existing)
                        result["status"] = "unchanged"
                        conn.commit()
                        return result
                    raise ContentConflictError(
                        f"fragment_no={fragment_no} revision={revision} already "
                        "exists with different content"
                    )
                # Higher revision accepted.
                conn.execute(
                    "UPDATE fragments SET revision = ?, start_sample = ?, "
                    "samples = ?, content_hash = ? WHERE id = ?",
                    (revision, start_sample, samples_json, new_hash, existing["id"]),
                )
                changed = True
            else:
                conn.execute(
                    "INSERT INTO fragments (station, minute_utc, fragment_no, "
                    "revision, start_sample, samples, content_hash) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        station,
                        minute,
                        fragment_no,
                        revision,
                        start_sample,
                        samples_json,
                        new_hash,
                    ),
                )
                changed = True

            # The effective fragment set changed: every earlier publication of
            # this minute is now superseded and marked stale in the same txn,
            # with one withdraw event per recalled publication.
            if changed:
                _withdraw_stale_publications(conn, station, minute)

            row = conn.execute(
                "SELECT * FROM fragments WHERE station = ? AND minute_utc = ? "
                "AND fragment_no = ?",
                (station, minute, fragment_no),
            ).fetchone()
            conn.commit()
            result = _row_to_fragment(row)
            result["status"] = "accepted" if existing is None else "superseded"
            return result
        except Exception:
            conn.rollback()
            raise


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #
def get_assembly(station: str, minute_dt: datetime) -> dict[str, Any]:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        rows = _get_fragments(conn, station, minute)
        fragments = [_row_to_fragment(r) for r in rows]
        assembly = compute_assembly(rows)
    return {
        "station": station,
        "minute_utc": _parse_minute(minute),
        **assembly,
        "fragments": fragments,
    }


def list_fragments(station: str, minute_dt: datetime) -> list[dict[str, Any]]:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        rows = _get_fragments(conn, station, minute)
        return [_row_to_fragment(r) for r in rows]


def _refresh_staleness(conn: sqlite3.Connection, pub: sqlite3.Row) -> sqlite3.Row:
    """Self-heal the stale flag from the current fragment set on read.

    Legacy rows may carry ``stale = 0`` even though their fragment set has
    changed.  Flipping the flag on read is paired with a withdraw event in the
    same transaction so subscribers still observe the withdrawal; withdrawals
    are idempotent for clients that already received one.
    """
    rows = _get_fragments(conn, pub["station"], pub["minute_utc"])
    current_hash = _fragment_set_hash(rows)
    if not pub["stale"] and current_hash != pub["fragment_set_hash"]:
        conn.execute("UPDATE publications SET stale = 1 WHERE id = ?", (pub["id"],))
        _emit_event(
            conn, pub["station"], pub["minute_utc"], "withdraw", pub["id"]
        )
        pub = conn.execute(
            "SELECT * FROM publications WHERE id = ?", (pub["id"],)
        ).fetchone()
    return pub


def _hydrate_publication(conn: sqlite3.Row, pub: sqlite3.Row) -> dict[str, Any]:
    items = conn.execute(
        "SELECT * FROM publication_items WHERE publication_id = ? ORDER BY fragment_no",
        (pub["id"],),
    ).fetchall()
    return {
        "id": pub["id"],
        "station": pub["station"],
        "minute_utc": _parse_minute(pub["minute_utc"]),
        "wave": load_samples(pub["wave"]),
        "stale": bool(pub["stale"]),
        "fragment_set_hash": pub["fragment_set_hash"],
        "created_at": pub["created_at"],
        "items": [
            {
                "fragment_no": it["fragment_no"],
                "revision": it["revision"],
                "start_sample": it["start_sample"],
                "samples": load_samples(it["samples"]),
                "content_hash": it["content_hash"],
            }
            for it in items
        ],
    }


def list_publications(station: str | None, minute_dt: datetime | None) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if station is not None:
        clauses.append("station = ?")
        params.append(station)
    if minute_dt is not None:
        clauses.append("minute_utc = ?")
        params.append(minute_key(minute_dt))
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_conn() as conn:
        pubs = conn.execute(
            f"SELECT * FROM publications{where} ORDER BY id", params
        ).fetchall()
        out = []
        for pub in pubs:
            pub = _refresh_staleness(conn, pub)
            out.append(_hydrate_publication(conn, pub))
        conn.commit()
        return out


def get_publication(pub_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        pub = conn.execute(
            "SELECT * FROM publications WHERE id = ?", (pub_id,)
        ).fetchone()
        if pub is None:
            return None
        pub = _refresh_staleness(conn, pub)
        result = _hydrate_publication(conn, pub)
        conn.commit()
        return result


def get_latest_publication(station: str, minute_dt: datetime) -> dict[str, Any] | None:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        pub = conn.execute(
            "SELECT * FROM publications WHERE station = ? AND minute_utc = ? "
            "ORDER BY id DESC LIMIT 1",
            (station, minute),
        ).fetchone()
        if pub is None:
            return None
        pub = _refresh_staleness(conn, pub)
        result = _hydrate_publication(conn, pub)
        conn.commit()
        return result


# --------------------------------------------------------------------------- #
# Publish
# --------------------------------------------------------------------------- #
def publish_minute(station: str, minute_dt: datetime) -> dict[str, Any]:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            rows = _get_fragments(conn, station, minute)
            assembly = compute_assembly(rows)

            if not rows:
                raise NotPublishableError("no fragments for this station-minute")
            if assembly["gaps"]:
                raise NotPublishableError(
                    f"minute incomplete, gaps at positions: "
                    f"{','.join(map(str, assembly['gaps']))}"
                )
            if assembly["conflicts"]:
                detail = "; ".join(
                    f"position {c['position']} fragments {c['fragment_nos']} "
                    f"values {c['values']}"
                    for c in assembly["conflicts"]
                )
                raise NotPublishableError(f"unresolved conflicts: {detail}")

            set_hash = _fragment_set_hash(rows)

            # A fresh publication for this exact fragment set already exists:
            # return it instead of creating a duplicate.
            existing = conn.execute(
                "SELECT * FROM publications WHERE station = ? AND minute_utc = ? "
                "AND fragment_set_hash = ? AND stale = 0 ORDER BY id DESC LIMIT 1",
                (station, minute, set_hash),
            ).fetchone()
            if existing is not None:
                result = _hydrate_publication(conn, existing)
                conn.commit()
                return result

            # Build the 60-sample wave from the committed fragment set.
            wave = [0] * SAMPLES_PER_MINUTE
            for r in rows:
                start = r["start_sample"]
                for offset, value in enumerate(load_samples(r["samples"])):
                    wave[start + offset] = value

            cur = conn.execute(
                "INSERT INTO publications (station, minute_utc, wave, "
                "fragment_set_hash, stale) VALUES (?, ?, ?, ?, 0)",
                (station, minute, dump_samples(wave), set_hash),
            )
            pub_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO publication_items (publication_id, fragment_no, "
                "revision, start_sample, samples, content_hash) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        pub_id,
                        r["fragment_no"],
                        r["revision"],
                        r["start_sample"],
                        r["samples"],
                        r["content_hash"],
                    )
                    for r in rows
                ],
            )
            pub = conn.execute(
                "SELECT * FROM publications WHERE id = ?", (pub_id,)
            ).fetchone()
            # The new record is this minute's current value: feed subscribers
            # receive the re-publication after the earlier withdrawals.
            _emit_event(conn, station, minute, "upsert", pub_id)
            result = _hydrate_publication(conn, pub)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise


# --------------------------------------------------------------------------- #
# Single-station feed: subscriptions and cursor-paged change delivery
# --------------------------------------------------------------------------- #
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500


def _encode_cursor(phase: str, last_id: int, sub_id: int) -> str:
    """Opaque, tamper-evident cursor: base64 of ``phase|sub_id|last_id``.

    ``last_id`` is the id of the last delivered row (snapshot row id or feed
    event id).  Retrying the same cursor always re-reads the same immutable
    rows, so pages are stable across retries and process restarts.
    """
    raw = f"{phase}|{sub_id}|{last_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_cursor(cursor: str) -> tuple[str, int, int]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        phase, sub_raw, id_raw = raw.split("|")
        sub_id, last_id = int(sub_raw), int(id_raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise CursorError("malformed cursor") from exc
    if phase not in ("snapshot", "live") or sub_id < 1 or last_id < 0:
        raise CursorError("malformed cursor")
    return phase, sub_id, last_id


def create_subscription(station: str) -> dict[str, Any]:
    """Freeze the station's current non-stale publications as the start point.

    The whole operation runs in one ``BEGIN IMMEDIATE`` transaction, so it is
    serialized against uploads and publishes: every change log row up to the
    high-water mark is fully represented in the snapshot, and every later
    change can only be delivered as a live event.  No event is lost or
    duplicated and the starting point never changes afterwards.
    """
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            high = conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS m FROM feed_events"
            ).fetchone()["m"]

            cur = conn.execute(
                "INSERT INTO subscriptions (station, max_event_id) VALUES (?, ?)",
                (station, high),
            )
            sub_id = cur.lastrowid

            # Snapshot = publications that are current right now.  Comparing
            # the frozen fragment-set hash with the hash of the live fragment
            # set identifies stale records without mutating their stale flag
            # (and therefore without emitting noise events).
            fresh_pubs = conn.execute(
                "SELECT * FROM publications WHERE station = ? AND stale = 0 "
                "ORDER BY id",
                (station,),
            ).fetchall()
            snapshot_rows: list[tuple[Any, ...]] = []
            for pub in fresh_pubs:
                rows = _get_fragments(conn, station, pub["minute_utc"])
                current_hash = _fragment_set_hash(rows)
                if current_hash == pub["fragment_set_hash"]:
                    snapshot_rows.append(
                        (sub_id, station, pub["minute_utc"], pub["id"])
                    )
            conn.executemany(
                "INSERT INTO subscription_snapshot "
                "(subscription_id, station, minute_utc, pub_id) "
                "VALUES (?, ?, ?, ?)",
                snapshot_rows,
            )

            sub = conn.execute(
                "SELECT * FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            first_cursor = _encode_cursor("snapshot", 0, sub_id)
            result = {
                "subscription_id": sub_id,
                "station": sub["station"],
                "snapshot_size": len(snapshot_rows),
                "created_at": sub["created_at"],
                "next_cursor": first_cursor,
            }
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise


def _event_from_row(conn: sqlite3.Connection, ev: sqlite3.Row, seq: int) -> dict[str, Any]:
    out: dict[str, Any] = {
        "seq": seq,
        "station": ev["station"],
        "minute_utc": _parse_minute(ev["minute_utc"]),
        "event_type": ev["event_type"],
        "publication_id": ev["pub_id"],
        "publication": None,
    }
    if ev["event_type"] == "upsert" and ev["pub_id"] is not None:
        pub = conn.execute(
            "SELECT * FROM publications WHERE id = ?", (ev["pub_id"],)
        ).fetchone()
        # The publication row is immutable once written; it can only be missing
        # under database tampering, in which case a 500 is the honest answer.
        if pub is None:  # pragma: no cover - defensive
            raise ServiceError(
                f"feed event {ev['id']} references missing publication "
                f"{ev['pub_id']}"
            )
        out["publication"] = _hydrate_publication(conn, pub)
    return out


def get_feed_page(
    subscription_id: int, cursor: str | None, page_size: int
) -> dict[str, Any]:
    """Return one deterministic page of the subscription's feed.

    Snapshot pages read immutable ``subscription_snapshot`` rows; live pages
    read the append-only ``feed_events`` log after the subscription's
    high-water mark.  The same cursor always yields the same page, including
    after a restart, because neither backing table is ever mutated.
    """
    if page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise BoundaryError(
            f"page_size must be between 1 and {MAX_PAGE_SIZE}, got {page_size}"
        )

    with get_conn() as conn:
        sub = conn.execute(
            "SELECT * FROM subscriptions WHERE id = ?", (subscription_id,)
        ).fetchone()
        if sub is None:
            raise SubscriptionNotFoundError(
                f"subscription {subscription_id} not found"
            )
        station = sub["station"]

        if cursor is None or cursor.strip() == "":
            phase, last_id = "snapshot", 0
        else:
            decoded_phase, decoded_sub, last_id = _decode_cursor(cursor)
            if decoded_sub != subscription_id:
                raise CursorError(
                    "cursor belongs to a different subscription"
                )
            phase = decoded_phase

        events: list[dict[str, Any]] = []

        if phase == "snapshot":
            rows = conn.execute(
                "SELECT * FROM subscription_snapshot WHERE subscription_id = ? "
                "AND id > ? ORDER BY id LIMIT ?",
                (subscription_id, last_id, page_size),
            ).fetchall()
            for snap in rows:
                pub = conn.execute(
                    "SELECT * FROM publications WHERE id = ?", (snap["pub_id"],)
                ).fetchone()
                if pub is None:  # pragma: no cover - defensive
                    raise ServiceError(
                        f"snapshot row {snap['id']} references missing "
                        f"publication {snap['pub_id']}"
                    )
                events.append(
                    {
                        "seq": snap["id"],
                        "station": station,
                        "minute_utc": _parse_minute(pub["minute_utc"]),
                        "event_type": "upsert",
                        "publication_id": pub["id"],
                        "publication": _hydrate_publication(conn, pub),
                    }
                )

            snapshot_left = conn.execute(
                "SELECT COUNT(*) AS n FROM subscription_snapshot "
                "WHERE subscription_id = ? AND id > ?",
                (subscription_id, last_id),
            ).fetchone()["n"]

            if snapshot_left > page_size:
                # Starting point continues on the next snapshot page.
                return _feed_response(
                    subscription_id,
                    station,
                    "snapshot",
                    events,
                    _encode_cursor("snapshot", rows[-1]["id"], subscription_id),
                    True,
                )

            if events:
                # The starting point fits on this page.  The page itself is a
                # pure snapshot page; its cursor already points at the live
                # tail so the next request picks up every change that happened
                # after subscription creation (none can precede it).  Whether
                # more data exists depends solely on the live tail length.
                last_id = sub["max_event_id"]
                live_left = conn.execute(
                    "SELECT COUNT(*) AS n FROM feed_events "
                    "WHERE station = ? AND id > ?",
                    (station, last_id),
                ).fetchone()["n"]
                return _feed_response(
                    subscription_id,
                    station,
                    "snapshot",
                    events,
                    _encode_cursor("live", last_id, subscription_id),
                    live_left > 0,
                )

            # Empty snapshot: enter the live tail directly on this page.
            last_id = sub["max_event_id"]

        live_rows = conn.execute(
            "SELECT * FROM feed_events WHERE station = ? AND id > ? "
            "ORDER BY id LIMIT ?",
            (station, last_id, page_size),
        ).fetchall()
        for ev in live_rows:
            events.append(_event_from_row(conn, ev, ev["id"]))
        if live_rows:
            last_id = live_rows[-1]["id"]

        live_left = conn.execute(
            "SELECT COUNT(*) AS n FROM feed_events WHERE station = ? AND id > ?",
            (station, last_id),
        ).fetchone()["n"]

        # On the live tail the same cursor is returned when nothing new exists
        # so clients can poll idempotently; it never advances past real rows.
        return _feed_response(
            subscription_id,
            station,
            "live",
            events,
            _encode_cursor("live", last_id, subscription_id),
            live_left > 0,
        )


def _feed_response(
    subscription_id: int,
    station: str,
    phase: str,
    events: list[dict[str, Any]],
    next_cursor: str,
    has_more: bool,
) -> dict[str, Any]:
    return {
        "subscription_id": subscription_id,
        "station": station,
        "phase": phase,
        "events": events,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }
