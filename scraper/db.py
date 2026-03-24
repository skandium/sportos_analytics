import duckdb
from datetime import datetime
from pathlib import Path

EXCLUDED_RESULTS_PATH = Path("data/excluded_results.txt")
EXCLUDED_EVENTS_PATH = Path("data/excluded_events.txt")


def _load_excluded_results() -> set[str]:
    if not EXCLUDED_RESULTS_PATH.exists():
        return set()
    excluded = set()
    for line in EXCLUDED_RESULTS_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        excluded.add(line)
    return excluded


def load_excluded_events(con=None) -> set[str]:
    if not EXCLUDED_EVENTS_PATH.exists():
        return set()
    excluded = set()
    for line in EXCLUDED_EVENTS_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.isdigit() and con is not None:
            row = con.execute(
                "SELECT id FROM (SELECT ROW_NUMBER() OVER (ORDER BY date, id) AS num, id FROM events) WHERE num = ?",
                [int(line)],
            ).fetchone()
            if row:
                excluded.add(row[0])
        else:
            excluded.add(line)
    return excluded


def init_db(path: str = "data/results.duckdb") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id          VARCHAR PRIMARY KEY,
            name        VARCHAR,
            date        DATE,
            url         VARCHAR,
            scraped_at  TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS distances (
            id          VARCHAR PRIMARY KEY,
            event_id    VARCHAR REFERENCES events(id),
            code        VARCHAR,
            label       VARCHAR,
            distance_m  INTEGER,
            url         VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id          VARCHAR PRIMARY KEY,
            distance_id VARCHAR REFERENCES distances(id),
            rank_overall INTEGER,
            rank_category INTEGER,
            bib         VARCHAR,
            name        VARCHAR,
            category    VARCHAR,
            gender      VARCHAR,
            time_seconds INTEGER,
            time_raw    VARCHAR,
            dnf         BOOLEAN DEFAULT FALSE
        )
    """)
    return con


def upsert_event(con, event: dict):
    con.execute("""
        INSERT OR REPLACE INTO events (id, name, date, url, scraped_at)
        VALUES (?, ?, ?, ?, ?)
    """, [
        event["id"],
        event["name"],
        event["date"],
        event["url"],
        datetime.now(),
    ])


def upsert_distance(con, distance: dict):
    con.execute("""
        INSERT OR REPLACE INTO distances (id, event_id, code, label, distance_m, url)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [
        distance["id"],
        distance["event_id"],
        distance["code"],
        distance["label"],
        distance.get("distance_m"),
        distance["url"],
    ])


def upsert_results(con, rows: list[dict]):
    if not rows:
        return
    excluded = _load_excluded_results()
    filtered = [r for r in rows if r["id"] not in excluded]
    if not filtered:
        return
    con.executemany("""
        INSERT OR REPLACE INTO results
            (id, distance_id, rank_overall, rank_category, bib, name, category, gender, time_seconds, time_raw, dnf)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (
            r["id"],
            r["distance_id"],
            r.get("rank_overall"),
            r.get("rank_category"),
            r["bib"],
            r["name"],
            r.get("category"),
            r.get("gender"),
            r.get("time_seconds"),
            r.get("time_raw"),
            r.get("dnf", False),
        )
        for r in filtered
    ])


def get_scraped_event_ids(con) -> set[str]:
    return set(row[0] for row in con.execute("SELECT id FROM events").fetchall())


def infer_gender(con) -> int:
    """Fill NULL gender from the same runner's other results.

    For each runner name that has at least one result with a known gender
    and at least one with NULL gender, set the NULL rows to the most
    common known gender for that name.

    Returns the number of rows updated.
    """
    before = con.execute(
        "SELECT count(*) FROM results WHERE gender IS NULL"
    ).fetchone()[0]

    con.execute("""
        UPDATE results
        SET gender = (
            SELECT r2.gender
            FROM results r2
            WHERE r2.name = results.name
              AND r2.gender IS NOT NULL
            GROUP BY r2.gender
            ORDER BY count(*) DESC
            LIMIT 1
        )
        WHERE gender IS NULL
          AND name IN (
              SELECT DISTINCT name FROM results
              WHERE gender IS NOT NULL
          )
    """)

    after = con.execute(
        "SELECT count(*) FROM results WHERE gender IS NULL"
    ).fetchone()[0]

    return before - after
