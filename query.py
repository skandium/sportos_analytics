"""Quick CLI to query the scraped results database."""

import argparse
import duckdb


def connect(db_path: str = "data/results.duckdb") -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path, read_only=True)


def stats(con):
    """Overview of what's in the database."""
    print("=== DB Stats ===")
    for table in ("events", "distances", "results"):
        count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count}")

    print("\n=== Events with result counts ===")
    rows = con.execute("""
        SELECT e.name, e.date, count(r.id) as results
        FROM events e
        JOIN distances d ON d.event_id = e.id
        LEFT JOIN results r ON r.distance_id = d.id
        GROUP BY e.id, e.name, e.date
        ORDER BY e.date DESC
    """).fetchall()
    for name, date, count in rows:
        print(f"  {date} | {count:>5} results | {name}")


def distances(con, event: str | None):
    """Show distances, optionally filtered by event slug substring."""
    if event:
        where = "WHERE e.id LIKE ?"
        params = [f"%{event}%"]
    else:
        where = ""
        params = []
    rows = con.execute(f"""
        SELECT e.name, d.label, d.distance_m, count(r.id)
        FROM distances d
        JOIN events e ON e.id = d.event_id
        LEFT JOIN results r ON r.distance_id = d.id
        {where}
        GROUP BY e.name, d.id, d.label, d.distance_m
        ORDER BY e.name, d.distance_m NULLS LAST
    """, params).fetchall()
    for ename, dlabel, dm, count in rows:
        dm_str = f"{dm}m" if dm else "?"
        print(f"  {ename} | {dlabel} ({dm_str}) | {count} results")


def results(con, event: str, limit: int):
    """Show results for an event."""
    rows = con.execute("""
        SELECT r.rank_overall, r.name, r.category, r.time_raw, d.label
        FROM results r
        JOIN distances d ON r.distance_id = d.id
        WHERE d.event_id LIKE ?
        ORDER BY d.label, r.rank_overall NULLS LAST
        LIMIT ?
    """, [f"%{event}%", limit]).fetchall()
    for rank, name, cat, time, dist in rows:
        rank_str = f"{rank:>4}" if rank else "   -"
        print(f"  {dist:20s} {rank_str}. {name} ({cat or '?'}) {time}")


def issues(con):
    """Show results with potential data issues."""
    print("=== Results with no time ===")
    rows = con.execute("""
        SELECT r.name, r.bib, r.time_raw, r.dnf, d.label, e.name as event
        FROM results r
        JOIN distances d ON r.distance_id = d.id
        JOIN events e ON e.id = d.event_id
        WHERE r.time_seconds IS NULL
        LIMIT 50
    """).fetchall()
    if not rows:
        print("  None")
    for name, bib, time_raw, dnf, dist, event in rows:
        status = "DNF" if dnf else "missing"
        print(f"  {event} | {dist} | {name} (#{bib}) time={time_raw!r} [{status}]")


def percentile(con, distance_m: int, time_seconds: int):
    """Show percentile rank for a given distance and time."""
    total = con.execute("""
        SELECT count(*) FROM results r
        JOIN distances d ON r.distance_id = d.id
        WHERE d.distance_m = ? AND r.time_seconds IS NOT NULL
    """, [distance_m]).fetchone()[0]

    if total == 0:
        print(f"No results found for {distance_m}m")
        return

    faster = con.execute("""
        SELECT count(*) FROM results r
        JOIN distances d ON r.distance_id = d.id
        WHERE d.distance_m = ? AND r.time_seconds < ? AND r.time_seconds IS NOT NULL
    """, [distance_m, time_seconds]).fetchone()[0]

    pct = (1 - faster / total) * 100
    mins, secs = divmod(time_seconds, 60)
    hours, mins = divmod(mins, 60)
    time_str = f"{hours}:{mins:02d}:{secs:02d}" if hours else f"{mins}:{secs:02d}"

    print(f"  Distance: {distance_m}m | Your time: {time_str}")
    print(f"  {faster}/{total} faster -> top {100 - pct:.1f}% (better than {pct:.1f}%)")


def sql(con, query: str):
    """Run arbitrary SQL."""
    rows = con.execute(query).fetchall()
    desc = con.description
    if desc:
        headers = [d[0] for d in desc]
        print("  " + " | ".join(headers))
        print("  " + "-" * (sum(len(h) + 3 for h in headers)))
    for row in rows:
        print("  " + " | ".join(str(v) for v in row))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query sportos.eu results database")
    parser.add_argument("--db", default="data/results.duckdb")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("stats", help="Show database overview")
    sub.add_parser("issues", help="Show data quality issues")

    p = sub.add_parser("distances", help="Show distances")
    p.add_argument("event", nargs="?", help="Filter by event slug substring")

    p = sub.add_parser("results", help="Show results for an event")
    p.add_argument("event", help="Event slug substring")
    p.add_argument("-n", type=int, default=20, help="Max rows")

    p = sub.add_parser("percentile", help="Rank a time against all results")
    p.add_argument("distance_m", type=int, help="Distance in meters (e.g. 5000, 10000)")
    p.add_argument("time", help="Time as H:MM:SS or MM:SS")

    p = sub.add_parser("sql", help="Run arbitrary SQL")
    p.add_argument("query", help="SQL query")

    args = parser.parse_args()
    con = connect(args.db)

    if args.cmd == "stats":
        stats(con)
    elif args.cmd == "distances":
        distances(con, args.event)
    elif args.cmd == "results":
        results(con, args.event, args.n)
    elif args.cmd == "issues":
        issues(con)
    elif args.cmd == "percentile":
        parts = args.time.split(":")
        if len(parts) == 3:
            t = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        else:
            t = int(parts[0]) * 60 + int(parts[1])
        percentile(con, args.distance_m, t)
    elif args.cmd == "sql":
        sql(con, args.query)
    else:
        parser.print_help()

    con.close()
