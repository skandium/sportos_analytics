import argparse
import logging
from datetime import date, timedelta

from tqdm import tqdm

from scraper.db import init_db, upsert_event, upsert_distance, upsert_results, get_scraped_event_ids, infer_gender, load_excluded_events
from scraper.events import discover_events, get_distances
from scraper.results import fetch_distance_html, parse_distance_html

log = logging.getLogger(__name__)


def scrape(since_years: int = 2, limit: int | None = None, db_path: str = "data/results.duckdb",
           fresh: bool = False, incremental: bool = False):
    """Discover events, fetch distances and save raw HTML. No parsing."""
    if fresh:
        import os
        if os.path.exists(db_path):
            os.remove(db_path)
            log.info("Deleted %s", db_path)
        wal = db_path + ".wal"
        if os.path.exists(wal):
            os.remove(wal)

    con = init_db(db_path)
    since_date = date.today() - timedelta(days=since_years * 365)
    existing = get_scraped_event_ids(con)

    log.info("Discovering events since %s (%d already in DB)", since_date, len(existing))
    events = discover_events(since_date, existing, limit=limit, stop_on_existing=incremental)

    if not events:
        log.info("No new events to scrape")
        con.close()
        return

    for event in tqdm(events, desc="Scraping"):
        upsert_event(con, event)

        distances = get_distances(event["id"])
        for dist in distances:
            upsert_distance(con, dist)
            fetch_distance_html(dist["id"], dist["url"])

    con.close()


def refetch(db_path: str = "data/results.duckdb"):
    """Re-fetch HTML for all distances already in the database."""
    con = init_db(db_path)

    distances = con.execute("SELECT id, url FROM distances ORDER BY id").fetchall()
    con.close()

    for dist_id, url in tqdm(distances, desc="Fetching"):
        fetch_distance_html(dist_id, url)


def parse(db_path: str = "data/results.duckdb", remove_excluded: bool = False, incremental: bool = False):
    """Parse all results from stored HTML into the database."""
    con = init_db(db_path)
    excluded_events = load_excluded_events(con)

    if remove_excluded and excluded_events:
        for event_id in excluded_events:
            deleted = con.execute("""
                DELETE FROM results WHERE distance_id IN (
                    SELECT id FROM distances WHERE event_id = ?
                )
            """, [event_id]).rowcount
            if deleted:
                log.info("Removed %d results for excluded event %s", deleted, event_id)

    if incremental:
        distances = con.execute("""
            SELECT d.id, d.event_id FROM distances d
            WHERE NOT EXISTS (SELECT 1 FROM results r WHERE r.distance_id = d.id)
            ORDER BY d.id
        """).fetchall()
        log.info("Incremental mode: %d unparsed distances", len(distances))
    else:
        distances = con.execute("SELECT id, event_id FROM distances ORDER BY id").fetchall()

    reparsed = 0
    for dist_id, event_id in tqdm(distances, desc="Parsing"):
        if event_id in excluded_events:
            continue
        results = parse_distance_html(dist_id)
        if results:
            con.execute("DELETE FROM results WHERE distance_id = ?", [dist_id])
            upsert_results(con, results)
            reparsed += 1

    log.info("Parsed %d distances", reparsed)

    updated = infer_gender(con)
    log.info("Gender inferred for %d results", updated)

    _print_stats(con)
    con.close()


def parse_event(event_ref: str, db_path: str = "data/results.duckdb", dry_run: bool = False):
    """Parse (or dry-run) results for a single event by number (from list-events)."""
    con = init_db(db_path)

    if event_ref.isdigit():
        row = con.execute(
            f"SELECT id, name, date FROM ({_EVENTS_NUMBERED}) WHERE num = ?", [int(event_ref)]
        ).fetchone()
    else:
        row = con.execute(
            "SELECT id, name, date FROM events WHERE id = ?", [event_ref]
        ).fetchone()

    if not row:
        log.error("Event '%s' not found. Use 'list-events' to find event numbers.", event_ref)
        con.close()
        return

    event_id, event_name, event_date = row

    if event_id in load_excluded_events(con):
        log.error("Event '%s' is excluded (listed in excluded_events.txt).", event_id)
        con.close()
        return
    log.info("Event: %s  %s  (%s)", event_date, event_name, event_id)

    distances = con.execute(
        "SELECT id, label, distance_m FROM distances WHERE event_id = ? ORDER BY distance_m NULLS LAST",
        [event_id],
    ).fetchall()

    for dist_id, label, distance_m in distances:
        results = parse_distance_html(dist_id)
        dm_str = f"{distance_m}m" if distance_m else "?"
        log.info("  %s (%s): %d results parsed", label, dm_str, len(results))

        if not results:
            continue

        # Show first/last few results for inspection
        for r in results[:5]:
            log.info("    #%-4s %-25s %s  (%s)", r["rank_overall"] or "-", r["name"], r["time_raw"], r["category"] or "")
        if len(results) > 10:
            log.info("    ...")
        for r in results[-5:] if len(results) > 10 else results[5:]:
            log.info("    #%-4s %-25s %s  (%s)", r["rank_overall"] or "-", r["name"], r["time_raw"], r["category"] or "")

        if not dry_run:
            con.execute("DELETE FROM results WHERE distance_id = ?", [dist_id])
            upsert_results(con, results)
            log.info("    -> saved to DB")

    if dry_run:
        log.info("Dry run — no changes written to DB")
    con.close()


_EVENTS_NUMBERED = """
    SELECT ROW_NUMBER() OVER (ORDER BY date, id) AS num, id, name, date
    FROM events
"""


def list_events(query: str = "", db_path: str = "data/results.duckdb"):
    """List events, optionally filtered by name/ID substring."""
    con = init_db(db_path)
    if query:
        rows = con.execute(
            f"SELECT num, id, name, date FROM ({_EVENTS_NUMBERED}) WHERE id ILIKE ? OR name ILIKE ? ORDER BY date DESC",
            [f"%{query}%", f"%{query}%"],
        ).fetchall()
    else:
        rows = con.execute(f"SELECT num, id, name, date FROM ({_EVENTS_NUMBERED}) ORDER BY date DESC").fetchall()

    if not rows:
        log.info("No events found.")
    for num, eid, ename, edate in rows:
        print(f"{num:>4}  {edate}  {ename}")
    con.close()


def _print_stats(con):
    events = con.execute("SELECT count(*) FROM events").fetchone()[0]
    distances = con.execute("SELECT count(*) FROM distances").fetchone()[0]
    results = con.execute("SELECT count(*) FROM results").fetchone()[0]
    log.info("DB stats: %d events, %d distances, %d results", events, distances, results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape sportos.eu running results")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sp_scrape = subparsers.add_parser("scrape", help="Discover events and fetch HTML")
    sp_scrape.add_argument("--limit", type=int, default=None, help="Max events to scrape")
    sp_scrape.add_argument("--years", type=int, default=2, help="How many years back")
    sp_scrape.add_argument("--db", default="data/results.duckdb")
    sp_scrape.add_argument("--fresh", action="store_true", help="Delete database and start from scratch")
    sp_scrape.add_argument("--incremental", action="store_true", help="Stop at first already-ingested event")

    sp_refetch = subparsers.add_parser("refetch", help="Re-fetch HTML for all existing distances")
    sp_refetch.add_argument("--db", default="data/results.duckdb")

    sp_parse = subparsers.add_parser("parse", help="Parse stored HTML into database")
    sp_parse.add_argument("--db", default="data/results.duckdb")
    sp_parse.add_argument("--remove-excluded", action="store_true", help="Delete existing results for excluded events")
    sp_parse.add_argument("--incremental", action="store_true", help="Only parse distances with no results yet")

    sp_list = subparsers.add_parser("list-events", help="List events (optionally filter by name/ID)")
    sp_list.add_argument("query", nargs="?", default="", help="Filter by name or ID substring")
    sp_list.add_argument("--db", default="data/results.duckdb")

    sp_parse_event = subparsers.add_parser("parse-event", help="Parse a single event by ID")
    sp_parse_event.add_argument("event", help="Event number (from list-events) or slug ID")
    sp_parse_event.add_argument("--db", default="data/results.duckdb")
    sp_parse_event.add_argument("--dry-run", action="store_true", help="Show parsed results without saving to DB")

    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command == "scrape":
        scrape(since_years=args.years, limit=args.limit, db_path=args.db, fresh=args.fresh, incremental=args.incremental)
    elif args.command == "refetch":
        refetch(db_path=args.db)
    elif args.command == "parse":
        parse(db_path=args.db, remove_excluded=args.remove_excluded, incremental=args.incremental)
    elif args.command == "list-events":
        list_events(args.query, db_path=args.db)
    elif args.command == "parse-event":
        parse_event(args.event, db_path=args.db, dry_run=args.dry_run)
