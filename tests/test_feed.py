"""Tests for the single-station waveform data feed.

Covers:
  * subscription start point freezes the non-stale publications at creation
  * paging delivers that immutable start point, including a mixed snapshot/live
    boundary page, then follows the change log on the live tail
  * corrections emit withdraw before the replacement upsert, never reordered
  * no event is lost or duplicated between the start point and the live tail
  * retrying a cursor (and restarting the process) returns the same page
  * feed events commit in the same transaction as the business change
  * the new tables are created on top of a pre-existing (legacy) database
"""
from __future__ import annotations

import base64
import sqlite3
import tempfile
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

@pytest.fixture()
def client(monkeypatch):
    tmpdir = tempfile.mkdtemp(prefix="wf-feed-")
    db_path = Path(tmpdir) / "test.db"
    monkeypatch.setenv("WAVEFORM_DB_PATH", str(db_path))

    import importlib

    from app import config, database, main, service

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    database.init_db()
    with TestClient(main.app) as c:
        yield c


def frag(no, rev, start, samples, station="STA1", minute=None):
    return {
        "station": station,
        "minute_utc": minute or MIN,
        "fragment_no": no,
        "revision": rev,
        "start_sample": start,
        "samples": samples,
    }


MIN = "2026-11-17T05:06:00Z"
MIN2 = "2026-11-17T05:07:00Z"


def publish_minute(client, minute, wave, station="STA1"):
    """Upload two covering fragments for ``wave`` and publish the minute."""
    client.post(
        "/fragments",
        json=frag(1, 1, 0, wave[:30], station=station, minute=minute),
    )
    client.post(
        "/fragments",
        json=frag(2, 1, 30, wave[30:], station=station, minute=minute),
    )
    r = client.post(
        "/publications", params={"station": station, "minute_utc": minute}
    )
    assert r.status_code == 201, r.text
    return r.json()


def drain(client, sub_id, page_size=100, cursor=None):
    """Read every page until the live tail is caught up; return all events."""
    events = []
    phases = []
    for _ in range(1000):
        params = {"page_size": page_size}
        if cursor is not None:
            params["cursor"] = cursor
        r = client.get(f"/subscriptions/{sub_id}/feed", params=params)
        assert r.status_code == 200, r.text
        page = r.json()
        phases.append(page["phase"])
        events.extend(page["events"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    return events, phases, cursor


# --------------------------------------------------------------------------- #
# Start point semantics
# --------------------------------------------------------------------------- #
def test_subscription_snapshot_freezes_current_publications(client):
    pub1 = publish_minute(client, MIN, list(range(60)))
    pub2 = publish_minute(client, MIN2, list(range(10, 70)))

    r = client.post("/subscriptions", params={"station": "STA1"})
    assert r.status_code == 201
    sub = r.json()
    assert sub["snapshot_size"] == 2
    assert sub["station"] == "STA1"

    events, phases, _ = drain(client, sub["subscription_id"])
    assert phases[0] == "snapshot"
    assert [e["publication_id"] for e in events] == [pub1["id"], pub2["id"]]
    assert all(e["event_type"] == "upsert" for e in events)
    # Following the last snapshot cursor enters the (empty) live tail.
    tail = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": _},
    ).json()
    assert tail["phase"] == "live" and tail["events"] == []
    # Full waveform and frozen publication body travel with each event.
    assert events[0]["publication"]["wave"] == list(range(60))
    assert events[1]["publication"]["wave"] == list(range(10, 70))


def test_stale_publications_are_excluded_from_snapshot(client):
    old = publish_minute(client, MIN, list(range(60)))
    # Correction invalidates the first publication; the minute is republished.
    new_wave = [99] + list(range(1, 60))
    client.post("/fragments", json=frag(1, 2, 0, new_wave[:30]))
    new = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()

    r = client.post("/subscriptions", params={"station": "STA1"})
    sub = r.json()
    assert sub["snapshot_size"] == 1
    events, _, _ = drain(client, sub["subscription_id"])
    assert [e["publication_id"] for e in events] == [new["id"]]
    assert old["id"] != new["id"]


def test_snapshot_never_changes_after_creation(client):
    pub = publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()

    # Uploads and publishes while the client is still paging must not alter
    # the frozen starting point.
    publish_minute(client, MIN2, list(range(5, 65)))
    correction = [8] + list(range(1, 60))
    client.post("/fragments", json=frag(1, 2, 0, correction[:30]))
    client.post("/publications", params={"station": "STA1", "minute_utc": MIN})

    page = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": sub["next_cursor"]},
    ).json()
    assert page["phase"] == "snapshot"
    assert len(page["events"]) == 1
    assert page["events"][0]["publication_id"] == pub["id"]
    assert page["events"][0]["publication"]["wave"] == list(range(60))


def test_subscription_between_withdraw_and_republish_does_not_skip(client):
    # Publish a complete minute, then correct it so the old publication is
    # withdrawn but the minute is not yet re-published.
    pub = publish_minute(client, MIN, list(range(60)))
    client.post(
        "/fragments", json=frag(1, 2, 0, [7] * 10)  # leaves gaps -> unpublishable
    )

    # Subscribe in the gap window: the stale publication is excluded from the
    # snapshot, and the withdraw event sits at/below the high-water mark, so
    # it must never be delivered as a live event either.
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    assert sub["snapshot_size"] == 0
    page = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": sub["next_cursor"]},
    ).json()
    assert page["phase"] == "live" and page["events"] == []

    # Restore full coverage and re-publish: only the fresh upsert appears.
    client.post(
        "/fragments", json=frag(1, 3, 0, [8] + list(range(1, 30)))
    )
    repub = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    events, _, _ = drain(
        client, sub["subscription_id"], cursor=page["next_cursor"]
    )
    assert [e["event_type"] for e in events] == ["upsert"]
    assert events[0]["publication_id"] == repub["id"]
    assert pub["id"] not in [e["publication_id"] for e in events]


def test_unknown_station_subscription_has_empty_snapshot_and_live_tail(client):
    sub = client.post("/subscriptions", params={"station": "GHOST"}).json()
    assert sub["snapshot_size"] == 0
    page = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": sub["next_cursor"]},
    ).json()
    assert page["phase"] == "live"
    assert page["events"] == []
    assert page["has_more"] is False


# --------------------------------------------------------------------------- #
# Withdraw / re-publish ordering
# --------------------------------------------------------------------------- #
def test_correction_emits_withdraw_before_republish(client):
    pub = publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    # Drain the starting point and keep the live-tail cursor.
    _, _, tail = drain(client, sub["subscription_id"])

    new_wave = [42] + list(range(1, 60))
    client.post("/fragments", json=frag(1, 2, 0, new_wave[:30]))
    repub = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()

    events, _, _ = drain(client, sub["subscription_id"], cursor=tail)
    assert [e["event_type"] for e in events] == ["withdraw", "upsert"]
    assert events[0]["publication_id"] == pub["id"]
    assert events[0]["publication"] is None
    assert events[1]["publication_id"] == repub["id"]
    assert events[1]["publication"]["wave"] == new_wave
    # Ordering is also visible in the monotonic seq values.
    assert events[0]["seq"] < events[1]["seq"]


def test_withdraw_happens_on_fragment_change_even_without_republish(client):
    pub = publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    _, _, tail = drain(client, sub["subscription_id"])

    # A correction that leaves gaps: minute no longer publishable, but the
    # previously delivered value must still be withdrawn.
    client.post("/fragments", json=frag(1, 2, 0, [7] * 10))
    events, _, _ = drain(client, sub["subscription_id"], cursor=tail)
    assert [e["event_type"] for e in events] == ["withdraw"]
    assert events[0]["publication_id"] == pub["id"]


def test_self_heal_stale_flag_emits_withdraw_event(client):
    # Simulate a legacy row: publication exists and is fresh, but its fragment
    # set changes without the eager staleness path (we force stale=0 back and
    # rely on the read-time self-heal to notice).
    pub = publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    _, _, tail = drain(client, sub["subscription_id"])

    client.post(
        "/fragments", json=frag(1, 2, 0, ([5] + list(range(1, 30))))
    )
    # Forcibly reset the stale flag as if an old-version writer had missed it.
    from app import config

    conn = sqlite3.connect(str(config.DB_PATH))
    conn.execute("UPDATE publications SET stale = 0 WHERE id = ?", (pub["id"],))
    # Also remove the withdraw event the eager path wrote, to prove the
    # self-heal mechanism alone re-creates it.
    conn.execute("DELETE FROM feed_events WHERE event_type = 'withdraw'")
    conn.commit()
    conn.close()

    # Any publication read self-heals the flag and must record a withdraw.
    r = client.get(f"/publications/{pub['id']}")
    assert r.json()["stale"] is True

    events, _, _ = drain(client, sub["subscription_id"], cursor=tail)
    assert [e["event_type"] for e in events] == ["withdraw"]
    assert events[0]["publication_id"] == pub["id"]


def test_idempotent_retry_upload_emits_no_events(client):
    publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    _, _, tail = drain(client, sub["subscription_id"])

    # Same revision + same content: the fragment set does not change.
    r = client.post("/fragments", json=frag(1, 1, 0, list(range(0, 30))))
    assert r.json()["status"] == "unchanged"
    page = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": tail},
    ).json()
    assert page["events"] == []


def test_feed_is_scoped_to_the_subscription_station(client):
    publish_minute(client, MIN, list(range(60)), station="STA1")
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    _, _, tail = drain(client, sub["subscription_id"])

    # Traffic on another station must never appear on STA1's feed.
    publish_minute(client, MIN, list(range(1, 61)), station="STA2")
    page = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": tail},
    ).json()
    assert page["events"] == []
    assert page["station"] == "STA1"


# --------------------------------------------------------------------------- #
# Paging, determinism and retries
# --------------------------------------------------------------------------- #
def test_paging_through_snapshot_and_live_tail(client):
    pubs = [
        publish_minute(client, MIN if i == 0 else f"2026-11-17T05:{7 + i:02d}:00Z",
                       list(range(i, i + 60)))
        for i in range(5)
    ]
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()

    # page_size=2: three snapshot pages (2+2+1); the last snapshot cursor
    # already points into the live tail.
    collected, phases, cursor = drain(
        client, sub["subscription_id"], page_size=2
    )
    assert phases == ["snapshot", "snapshot", "snapshot"]
    assert [e["publication_id"] for e in collected] == [p["id"] for p in pubs]

    # Following the final snapshot cursor enters the empty live tail.
    tail = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": cursor},
    ).json()
    assert tail["phase"] == "live" and tail["events"] == []

    # Retrying every earlier cursor must replay exactly the same pages.
    cursors = [sub["next_cursor"]]
    cur = sub["next_cursor"]
    for _ in range(4):
        page = client.get(
            f"/subscriptions/{sub['subscription_id']}/feed",
            params={"cursor": cur, "page_size": 2},
        ).json()
        cur = page["next_cursor"]
        cursors.append(cur)

    replayed = []
    for cur in cursors[:-1]:
        page = client.get(
            f"/subscriptions/{sub['subscription_id']}/feed",
            params={"cursor": cur, "page_size": 2},
        ).json()
        replayed.extend(e["publication_id"] for e in page["events"])
    assert replayed == [p["id"] for p in pubs]


def test_snapshot_to_live_boundary_uses_distinct_pages(client):
    pubs = [
        publish_minute(client, MIN, list(range(60))),
        publish_minute(client, MIN2, list(range(1, 61))),
    ]
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    # A change arrives after subscription creation but before paging starts.
    correction = [3] + list(range(1, 60))
    client.post("/fragments", json=frag(1, 2, 0, correction[:30], minute=MIN))
    repub = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()

    # Snapshot page: only the frozen start point, never the later changes.
    page = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": sub["next_cursor"], "page_size": 3},
    ).json()
    assert page["phase"] == "snapshot"
    assert [(e["event_type"], e["publication_id"]) for e in page["events"]] == [
        ("upsert", pubs[0]["id"]),
        ("upsert", pubs[1]["id"]),
    ]
    # The snapshot page's cursor already points into the live tail.
    nxt = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": page["next_cursor"], "page_size": 3},
    ).json()
    assert nxt["phase"] == "live"
    assert [e["event_type"] for e in nxt["events"]] == ["withdraw", "upsert"]
    assert nxt["events"][0]["publication_id"] == pubs[0]["id"]
    assert nxt["events"][1]["publication_id"] == repub["id"]


def test_retrying_cursor_returns_identical_page(client):
    publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    page1 = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": sub["next_cursor"], "page_size": 1},
    ).json()
    page2 = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": sub["next_cursor"], "page_size": 1},
    ).json()
    assert page1 == page2
    # The last (here, only) snapshot page reports has_more from the live tail,
    # which is empty right after subscription creation.
    assert page1["phase"] == "snapshot"
    assert page1["has_more"] is False

    # Retrying that snapshot cursor later, after changes arrive, still replays
    # the identical frozen snapshot page.
    client.post("/fragments", json=frag(1, 2, 0, ([5] + list(range(1, 30)))))
    client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    page3 = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": sub["next_cursor"], "page_size": 1},
    ).json()
    assert page3["phase"] == "snapshot"
    assert [e["publication_id"] for e in page3["events"]] == [
        e["publication_id"] for e in page1["events"]
    ]
    assert page3["events"][0]["publication"]["wave"] == list(range(60))

    # Same on the live tail.
    tail = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": page1["next_cursor"], "page_size": 1},
    ).json()
    tail_retry = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": page1["next_cursor"], "page_size": 1},
    ).json()
    assert tail == tail_retry


def test_live_tail_polling_without_new_events_is_stable_empty_page(client):
    publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    events, _, cursor = drain(client, sub["subscription_id"])
    assert len(events) == 1

    page = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": cursor},
    ).json()
    assert page["has_more"] is False
    assert page["events"] == []
    # Polling again with the returned cursor gives the same stable page.
    again = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": page["next_cursor"]},
    ).json()
    assert again["events"] == []
    assert again["next_cursor"] == page["next_cursor"]


def test_cursor_validation(client):
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    # Garbage cursor.
    r = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": "not-a-cursor"},
    )
    assert r.status_code == 400
    assert "malformed" in r.json()["detail"]

    # Cursor forged for a different subscription.
    foreign = base64.urlsafe_b64encode(b"live|9999|0").decode()
    r = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": foreign},
    )
    assert r.status_code == 400
    assert "different subscription" in r.json()["detail"]

    # Unknown subscription id.
    r = client.get("/subscriptions/424242/feed")
    assert r.status_code == 404

    # Out-of-range page size is rejected by validation.
    r = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"page_size": 0},
    )
    assert r.status_code == 422


def test_feed_survives_restart(client, monkeypatch):
    pub = publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    sub_id = sub["subscription_id"]
    snapshot_cursor = sub["next_cursor"]

    import importlib

    from app import config, database, main, service

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    with TestClient(main.app) as restarted:
        # The frozen snapshot is still paged identically after restart.
        page = restarted.get(
            f"/subscriptions/{sub_id}/feed",
            params={"cursor": snapshot_cursor, "page_size": 1},
        ).json()
        assert page["phase"] == "snapshot"
        assert page["events"][0]["publication_id"] == pub["id"]
        assert page["events"][0]["publication"]["wave"] == list(range(60))

        # Advance to the live tail before producing new changes.
        _, _, tail = drain(restarted, sub_id)

        # And a new correction still flows through the live tail in order.
        new_wave = [5] + list(range(1, 60))
        restarted.post("/fragments", json=frag(1, 2, 0, new_wave[:30]))
        repub = restarted.post(
            "/publications", params={"station": "STA1", "minute_utc": MIN}
        ).json()
        events, _, _ = drain(restarted, sub_id, cursor=tail)
        assert [e["event_type"] for e in events] == ["withdraw", "upsert"]
        assert events[1]["publication_id"] == repub["id"]


# --------------------------------------------------------------------------- #
# No loss / no duplication across the snapshot/live boundary
# --------------------------------------------------------------------------- #
def test_no_events_lost_between_snapshot_and_live_tail(client):
    # Three minutes published before subscribing.
    pre = [
        publish_minute(
            client, f"2026-11-17T05:{6 + i:02d}:00Z", list(range(i, i + 60))
        )
        for i in range(3)
    ]
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()

    # Correct each pre-existing minute once and add one brand-new minute.
    replacements = []
    for i, pub in enumerate(pre):
        minute = f"2026-11-17T05:{6 + i:02d}:00Z"
        wave = [100 + i] + list(range(1, 60))
        client.post(
            "/fragments", json=frag(1, 2, 0, wave[:30], minute=minute)
        )
        replacements.append(
            client.post(
                "/publications", params={"station": "STA1", "minute_utc": minute}
            ).json()
        )
    fresh = publish_minute(client, "2026-11-17T05:09:00Z", list(range(9, 69)))

    events, _, _ = drain(client, sub["subscription_id"], page_size=2)
    ids = [e["publication_id"] for e in events]
    types = [e["event_type"] for e in events]

    # Every pre-existing publication appears once as a snapshot upsert and,
    # after correction, once as withdraw + replacement upsert.
    for pub in pre:
        assert ids.count(pub["id"]) == 2  # snapshot upsert + withdraw
    for repub in replacements:
        assert ids.count(repub["id"]) == 1
        assert types[ids.index(repub["id"])] == "upsert"
    assert ids.count(fresh["id"]) == 1
    # Each withdraw strictly precedes its replacement upsert.
    for pub, repub in zip(pre, replacements):
        assert ids.index(pub["id"], len(pre)) < ids.index(repub["id"])
    # seq values are strictly increasing over the whole feed.
    seqs = [e["seq"] for e in events]
    assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))


# --------------------------------------------------------------------------- #
# Atomicity: business change and feed events share one transaction
# --------------------------------------------------------------------------- #
def test_failed_publish_leaves_no_events(client):
    client.post("/fragments", json=frag(1, 1, 0, [1, 2, 3]))
    r = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    )
    assert r.status_code == 409
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    page = client.get(
        f"/subscriptions/{sub['subscription_id']}/feed",
        params={"cursor": sub["next_cursor"]},
    ).json()
    assert page["events"] == []


def test_event_log_rows_are_append_only_and_never_mutated(client):
    pub = publish_minute(client, MIN, list(range(60)))
    sub = client.post("/subscriptions", params={"station": "STA1"}).json()
    client.post("/fragments", json=frag(1, 2, 0, ([9] + list(range(1, 30)))))
    client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    events, _, _ = drain(client, sub["subscription_id"])

    from app import config

    conn = sqlite3.connect(str(config.DB_PATH))
    try:
        rows = conn.execute(
            "SELECT event_type, pub_id FROM feed_events ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [
        ("upsert", pub["id"]),
        ("withdraw", pub["id"]),
        ("upsert", events[-1]["publication_id"]),
    ]


# --------------------------------------------------------------------------- #
# Compatibility: new tables appear on a database created by the old version
# --------------------------------------------------------------------------- #
def test_schema_upgrades_legacy_database(monkeypatch, tmp_path):
    db_path = tmp_path / "legacy.db"
    # Build a database with only the original three tables.
    legacy_schema = """
    PRAGMA journal_mode = WAL;
    CREATE TABLE fragments (
        id INTEGER PRIMARY KEY AUTOINCREMENT, station TEXT NOT NULL,
        minute_utc TEXT NOT NULL, fragment_no INTEGER NOT NULL,
        revision INTEGER NOT NULL CHECK (revision >= 1),
        start_sample INTEGER NOT NULL, samples TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        UNIQUE (station, minute_utc, fragment_no));
    CREATE TABLE publications (
        id INTEGER PRIMARY KEY AUTOINCREMENT, station TEXT NOT NULL,
        minute_utc TEXT NOT NULL, wave TEXT NOT NULL,
        fragment_set_hash TEXT NOT NULL, stale INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')));
    CREATE TABLE publication_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT, publication_id INTEGER NOT NULL,
        fragment_no INTEGER NOT NULL, revision INTEGER NOT NULL,
        start_sample INTEGER NOT NULL, samples TEXT NOT NULL,
        content_hash TEXT NOT NULL);
    """
    conn = sqlite3.connect(str(db_path))
    conn.executescript(legacy_schema)
    conn.commit()
    conn.close()

    monkeypatch.setenv("WAVEFORM_DB_PATH", str(db_path))
    import importlib

    from app import config, database, main, service

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    database.init_db()  # must add the feed tables without touching old data

    with TestClient(main.app) as c:
        # Legacy publication exists with no feed history; subscription still
        # works and simply starts from an empty change log.
        pub = publish_minute(c, MIN, list(range(60)))
        sub = c.post("/subscriptions", params={"station": "STA1"}).json()
        events, _, _ = drain(c, sub["subscription_id"])
        assert [e["publication_id"] for e in events] == [pub["id"]]


# --------------------------------------------------------------------------- #
# Concurrency: subscriptions serialize with writes, no half state
# --------------------------------------------------------------------------- #
def test_concurrent_corrections_keep_withdraw_before_upsert(monkeypatch):
    import socket
    import time

    import httpx
    import uvicorn

    tmpdir = tempfile.mkdtemp(prefix="wf-feedlive-")
    db_path = Path(tmpdir) / "live.db"
    monkeypatch.setenv("WAVEFORM_DB_PATH", str(db_path))

    import importlib

    from app import config, database, main, service

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    database.init_db()

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    server = uvicorn.Server(
        uvicorn.Config(main.app, host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            if httpx.get(base + "/health", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.1)
    else:  # pragma: no cover
        raise RuntimeError("test server did not start")

    try:
        with httpx.Client(base_url=base, timeout=15) as http:
            # Baseline publication.
            http.post(
                "/fragments",
                json=frag(1, 1, 0, list(range(0, 30))),
            )
            http.post(
                "/fragments",
                json=frag(2, 1, 30, list(range(30, 60))),
            )
            pub = http.post(
                "/publications", params={"station": "STA1", "minute_utc": MIN}
            ).json()
            sub = http.post(
                "/subscriptions", params={"station": "STA1"}
            ).json()

            # N threads contend for the SQLite write lock simultaneously.
            # Each corrects its own unique fragment number at one distinct
            # slot and writes the SAME value the baseline already had there,
            # so overlaps never become value conflicts and every intermediate
            # state stays publishable.  BEGIN IMMEDIATE still serializes the
            # transactions: each correction withdraws the current publication
            # and each republish emits its replacement in commit order.
            n_threads = 5
            start = threading.Barrier(n_threads)
            errors: list[Exception] = []

            def worker(idx: int) -> None:
                try:
                    no = idx + 3  # fragment numbers 3..7
                    rev = idx + 2  # distinct higher revisions
                    start.wait()
                    r = http.post(
                        "/fragments",
                        json=frag(no, rev, idx, [idx]),
                    )
                    assert r.status_code == 201, r.text
                    rp = http.post(
                        "/publications",
                        params={"station": "STA1", "minute_utc": MIN},
                    )
                    assert rp.status_code in (201, 409), rp.text
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert not errors

            # The minute stays complete and conflict-free throughout, so the
            # last republish (or this one) yields a fresh current publication.
            final_pub = http.post(
                "/publications", params={"station": "STA1", "minute_utc": MIN}
            ).json()
            assert final_pub["stale"] is False

            # Page from the frozen start all the way to the caught-up live
            # tail, collecting every event.  Uses raw httpx calls because the
            # module-level drain helper expects a TestClient.
            collected: list[dict] = []
            snapshot_count = 0
            cursor = None
            for _ in range(100):
                params = {"page_size": 4}
                if cursor is not None:
                    params["cursor"] = cursor
                page = http.get(
                    f"/subscriptions/{sub['subscription_id']}/feed",
                    params=params,
                ).json()
                if page["phase"] == "snapshot":
                    snapshot_count += len(page["events"])
                else:
                    collected.extend(page["events"])
                cursor = page["next_cursor"]
                if page["phase"] == "live" and not page["has_more"]:
                    break
            assert snapshot_count == 1  # exactly the baseline publication

            kinds = [e["event_type"] for e in collected]

            # The original publication is always withdrawn (the very first
            # committed correction does it) and the stream ends on an upsert.
            assert kinds[0] == "withdraw"
            assert kinds[-1] == "upsert"
            # Every upsert is preceded by its withdraw; the running balance
            # (withdraw=-1, upsert=+1) can never show two live upserts ahead.
            balance = 0
            for kind in kinds:
                balance += -1 if kind == "withdraw" else 1
                assert balance <= 1
            # Withdrawals and replacements pair up exactly.
            assert kinds.count("withdraw") == kinds.count("upsert")
            # Event order is the global commit order: unique, increasing seq.
            seqs = [e["seq"] for e in collected]
            assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))
            # The original publication id appears among the withdrawals.
            assert pub["id"] in [
                e["publication_id"]
                for e in collected
                if e["event_type"] == "withdraw"
            ]
            # The final upsert delivers the complete, corrected wave.
            final_upsert = [e for e in collected
                            if e["event_type"] == "upsert"][-1]
            assert final_upsert["publication_id"] == final_pub["id"]
            assert len(final_upsert["publication"]["wave"]) == 60
    finally:
        server.should_exit = True
        thread.join(timeout=10)
