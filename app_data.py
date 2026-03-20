"""Data access, SQL helpers, and ranking functions for the Streamlit app."""

import duckdb
from pathlib import Path

DB_PATH = "data/results.duckdb"
EXCLUDED_RUNNERS_PATH = Path("data/excluded_runners.txt")
NAME_MERGES_PATH = Path("data/name_merges.txt")


# --- Excluded runners ---

def _load_excluded_runners():
    if not EXCLUDED_RUNNERS_PATH.exists():
        return set()
    names = set()
    for line in EXCLUDED_RUNNERS_PATH.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.add(line)
    return names


EXCLUDED_RUNNERS = _load_excluded_runners()


# --- DB connection ---

def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


# --- Name merges ---

def _load_name_merges():
    if not NAME_MERGES_PATH.exists():
        return {}
    merges = {}
    for line in NAME_MERGES_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if " -> " not in line:
            continue
        variant, canonical = line.split(" -> ", 1)
        merges[variant.strip()] = canonical.strip()
    return merges


NAME_MERGE = _load_name_merges()
_NM_CASES = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in NAME_MERGE.items())
NM = f"(CASE r.name {_NM_CASES} ELSE r.name END)" if NAME_MERGE else "r.name"


# --- Distance merges ---

DISTANCE_MERGE = {
    21000: 21100,   # half marathon
    42000: 42195,   # marathon
    42200: 42195,   # marathon
}
_DM_CASES = " ".join(f"WHEN {k} THEN {v}" for k, v in DISTANCE_MERGE.items())
DM = f"(CASE d.distance_m {_DM_CASES} ELSE d.distance_m END)"


# --- Pace filters ---

MIN_PACE_BY_DISTANCE = {
    42195: 170,  # marathon WR ~2:52/km, cutoff 2:50/km
    21100: 165,  # half marathon WR ~2:43/km, cutoff 2:45/km
    10000: 155,  # 10K WR ~2:36/km, cutoff 2:35/km
}
DEFAULT_MIN_PACE = 150  # default ~2:30/km for shorter distances
MEDIAN_PACE_CUTOFF = 210  # 3:30/km in seconds


def _min_time_for_distance(distance_m):
    """Return minimum realistic time in seconds for a given distance."""
    pace = DEFAULT_MIN_PACE
    for threshold in sorted(MIN_PACE_BY_DISTANCE):
        if distance_m >= threshold:
            pace = MIN_PACE_BY_DISTANCE[threshold]
    return int(distance_m / 1000 * pace)


# --- Race-level filters ---

def _excluded_distance_ids_sql():
    """SQL subquery that returns distance IDs where pace is unrealistic."""
    return f"""
        SELECT d.id
        FROM results r
        JOIN distances d ON r.distance_id = d.id
        WHERE d.distance_m IS NOT NULL AND r.time_seconds IS NOT NULL AND r.time_seconds > 0
        GROUP BY d.id, d.distance_m
        HAVING median(r.time_seconds) * 1.0 / (d.distance_m / 1000.0) < {MEDIAN_PACE_CUTOFF}
            OR percentile_cont(0.05) WITHIN GROUP (ORDER BY r.time_seconds) * 1.0 / (d.distance_m / 1000.0) < {DEFAULT_MIN_PACE}
        UNION ALL
        SELECT d.id FROM distances d WHERE d.label ILIKE '%virtuaal%'
            OR d.label ILIKE '%kombo%'
            OR d.label ILIKE '%teate%'
    """


GOOD_RACE = f"AND d.id NOT IN ({_excluded_distance_ids_sql()})"

DATE_12M = "AND d.event_id IN (SELECT e.id FROM events e WHERE e.date >= current_date - INTERVAL '12 months')"


# --- Excluded runners SQL ---

def _excluded_runners_sql():
    """SQL IN-list of excluded runner names."""
    if not EXCLUDED_RUNNERS:
        return ""
    names = ", ".join(f"'{n.replace(chr(39), chr(39)+chr(39))}'" for n in EXCLUDED_RUNNERS)
    return f"AND {NM} NOT IN ({names})"


# --- PB queries ---

def _clean_pb_sql(dist_m, extra_where=""):
    """SQL subquery: outlier-filtered PBs at a given distance. Returns (name, clean_best)."""
    min_time = _min_time_for_distance(dist_m)
    excluded = _excluded_runners_sql()
    NM3 = NM.replace('r.', 'r3.')
    return f"""
        SELECT name,
               CASE WHEN second_best IS NOT NULL AND best < second_best * 0.7
                    THEN second_best ELSE best
               END AS clean_best
        FROM (
            SELECT {NM} AS name,
                   min(r.time_seconds) AS best,
                   min(r.time_seconds) FILTER (
                       WHERE r.time_seconds > (
                           SELECT min(r3.time_seconds) FROM results r3
                           JOIN distances d3 ON r3.distance_id = d3.id
                           WHERE {NM3} = {NM} AND {DM.replace('d.', 'd3.')} = {dist_m}
                             AND r3.time_seconds >= {min_time} AND r3.dnf = false
                       )
                   ) AS second_best
            FROM results r
            JOIN distances d ON r.distance_id = d.id
            WHERE {DM} = {dist_m} AND r.time_seconds >= {min_time}
              AND r.dnf = false AND {NM} != '' {GOOD_RACE} {excluded} {extra_where}
            GROUP BY {NM}
        )
    """


# --- Runner lists ---

def _get_all_runners_at_distance(con, dist_m, extra_where="", include_predicted=True):
    """Get all runners' best times at a distance, with optional Riegel predictions.

    Returns sorted list of (name, time_seconds, source) tuples.
    """
    from scraper.predict import CANONICAL, CANONICAL_LABELS, predict_time

    actual = {}
    for name, time_s in con.execute(
        f"SELECT name, clean_best FROM ({_clean_pb_sql(dist_m, extra_where)})"
    ).fetchall():
        actual[name] = time_s

    if not include_predicted:
        result = [(n, t, "actual") for n, t in actual.items()]
        result.sort(key=lambda x: x[1])
        return result

    predicted = {}
    for other_dist in (d for d in CANONICAL if d != dist_m):
        for name, best_s in con.execute(
            f"SELECT name, clean_best FROM ({_clean_pb_sql(other_dist, extra_where)})"
        ).fetchall():
            if name in actual:
                continue
            pred_s = predict_time(other_dist, best_s, dist_m)
            if name not in predicted or pred_s < predicted[name][0]:
                predicted[name] = (pred_s, f"ennustus: {CANONICAL_LABELS[other_dist]}")

    result = [(n, t, "actual") for n, t in actual.items()]
    result += [(n, t, src) for n, (t, src) in predicted.items()]
    result.sort(key=lambda x: x[1])
    return result


# --- Ranking ---

def _ranked_runners(con, dist_m, gender=None, date_where=""):
    """Single entry point for ranked runner lists. All pages call this."""
    gender_sql = f"AND r.gender = '{gender}'" if gender else ""
    return _get_all_runners_at_distance(con, dist_m,
        extra_where=f"{gender_sql} {date_where}", include_predicted=False)


def _rank_for_time(runners, time_s):
    """Standard competition ranking: count strictly faster + 1."""
    return sum(1 for _, t, _ in runners if t < time_s) + 1, len(runners)


def _find_runner_rank(runners, runner_names):
    """Find a runner in ranked list, return (rank, time_s, name, total) or None."""
    for name, time_s, source in runners:
        if name in runner_names:
            rank, total = _rank_for_time(runners, time_s)
            return rank, time_s, name, total
    return None


# --- Helpers ---

def _format_distance(m):
    """Format distance in meters to a human-readable label."""
    if m >= 1000 and m % 1000 == 0:
        return f"{m // 1000} km"
    elif m >= 1000 and m % 100 == 0:
        return f"{m / 1000:.1f} km"
    elif m >= 1000:
        return f"{m / 1000:.2f} km"
    else:
        return f"{m} m"


# --- Runner PBs ---

def _build_runner_pbs(con, runner_names, runner_gender, date_where=""):
    """Build PB table data for a set of runner names.

    Returns list of dicts for _styled_pb_table, or empty list.
    """
    from scraper.predict import CANONICAL, CANONICAL_LABELS, predict_time, format_time as fmt_time

    actual_pbs = {}  # dist_m -> (name, time_s)
    actual_rows = {}  # dist_m -> pb_data dict
    for dist_m in CANONICAL:
        all_runners = _ranked_runners(con, dist_m, date_where=date_where)
        found = _find_runner_rank(all_runners, runner_names)

        if found is None:
            continue

        all_rank, best_s, r_name, all_total = found
        actual_pbs[dist_m] = (r_name, best_s)
        event_row = con.execute(f"""
            SELECT r.time_raw, r.gender, e.name, extract('year' FROM e.date), e.url
            FROM results r
            JOIN distances d ON r.distance_id = d.id
            JOIN events e ON e.id = d.event_id
            WHERE {NM} = ? AND r.time_seconds = ? AND {DM} = ? AND r.dnf = false
              {date_where}
            LIMIT 1
        """, [r_name, best_s, dist_m]).fetchone()
        races = con.execute(f"""
            SELECT count(*) FROM results r
            JOIN distances d ON r.distance_id = d.id
            JOIN events e ON e.id = d.event_id
            WHERE {NM} = ? AND {DM} = ? AND r.dnf = false
              {date_where}
        """, [r_name, dist_m]).fetchone()[0]
        time_display = event_row[0] if event_row else fmt_time(best_s)
        pb_has_gender = event_row[1] is not None if event_row else False
        event_str = f"{event_row[2]} ({int(event_row[3])})" if event_row else ""
        results_url = event_row[4].rstrip("/") + "/tulemused/" if event_row and event_row[4] else ""

        gender_rank_str = ""
        if runner_gender and pb_has_gender:
            g_runners = _ranked_runners(con, dist_m, gender=runner_gender, date_where=date_where)
            g_rank, g_total = _rank_for_time(g_runners, best_s)
            gender_rank_str = f"{g_rank:,} / {g_total:,}"

        pace_s = best_s / (dist_m / 1000)
        pace_str = f"{int(pace_s // 60)}:{int(pace_s % 60):02d}"

        gender_label = f"koht ({runner_gender})" if runner_gender else "koht (sugu)"
        actual_rows[dist_m] = {
            "distants": _format_distance(dist_m),
            "aeg": time_display,
            "võistlus": event_str,
            "tempo (min/km)": pace_str,
            gender_label: gender_rank_str,
            "koht (kõik)": f"{all_rank:,} / {all_total:,}",
            "võistlusi": races,
            "_event_url": results_url,
        }

    # Pass 2: build pb_data with predictions for missing distances
    pb_data = []
    gender_label = f"koht ({runner_gender})" if runner_gender else "koht (sugu)"
    for dist_m in CANONICAL:
        if dist_m in actual_rows:
            pb_data.append(actual_rows[dist_m])
        elif actual_pbs:
            best_pred = None
            best_src = None
            for src_dist, (_, src_time) in actual_pbs.items():
                pred = predict_time(src_dist, src_time, dist_m)
                if best_src is None or abs(src_dist - dist_m) < abs(best_src - dist_m):
                    best_pred = pred
                    best_src = src_dist

            pace_s = best_pred / (dist_m / 1000)
            pace_str = f"{int(pace_s // 60)}:{int(pace_s % 60):02d}"

            pb_data.append({
                "distants": _format_distance(dist_m),
                "aeg": f"~{fmt_time(best_pred)}",
                "võistlus": f"ennustus ({CANONICAL_LABELS[best_src]})",
                "tempo (min/km)": pace_str,
                gender_label: "",
                "koht (kõik)": "",
                "võistlusi": 0,
                "_event_url": "",
            })

    return pb_data
