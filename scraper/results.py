import re
import logging
from pathlib import Path

from bs4 import BeautifulSoup

from scraper.http import fetch

log = logging.getLogger(__name__)

HTML_DIR = Path("data/html")


def _parse_time(time_str: str) -> int | None:
    """Convert 'HH:MM:SS', 'MM:SS', or variants with decimals (.d) to total seconds."""
    time_str = time_str.strip()
    if not time_str:
        return None
    parts = time_str.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2]))
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(float(parts[1]))
    except ValueError:
        return None
    return None


def _extract_gender(category: str) -> str | None:
    """Extract gender from category like 'M40', 'N35', 'M', 'N',
    'Tüdrukud TA', 'Poisid PA', 'U 23 naised', 'P U19', 'T16', etc."""
    if not category:
        return None
    cat = category.strip()
    if not cat:
        return None

    # Standard M/N prefix (M, M40, N35, NU16, MU20, ...)
    # Guard: skip words like "Masters", "Noored" (alpha after M/N that isn't U)
    if cat[0] in ("M", "N") and (len(cat) == 1 or not cat[1:2].isalpha() or cat[1:2] == "U"):
        return cat[0]

    low = cat.lower()

    # Estonian keywords: naised/nais = women, tüdruk = girl
    if "nais" in low or "tüdruk" in low:
        return "N"
    # Estonian keywords: mehed/mees = men, poisi/poisid = boys
    if "mees" in low or "meh" in low or "pois" in low:
        return "M"

    # P/T prefix: P = poiss (boy), T = tüdruk (girl)
    # Matches: P, P16, P18, P U14, P U19, P 2015, T, T16, T U14, etc.
    if cat[0] == "P" and (len(cat) == 1 or not cat[1:].strip().isalpha()):
        return "M"
    if cat[0] == "T" and (len(cat) == 1 or not cat[1:].strip().isalpha()):
        return "N"

    return None


def _html_dir(distance_id: str) -> Path:
    return HTML_DIR / distance_id


def fetch_distance_html(distance_id: str, distance_url: str) -> Path:
    """Fetch all result pages for a distance and save raw HTML to disk.

    Returns the directory where HTML files are stored.
    """
    out_dir = _html_dir(distance_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_url = distance_url.rstrip("/")
    page = 0

    while True:
        url = f"{base_url}/?page={page}" if page > 0 else distance_url
        resp = fetch(url)
        html = resp.text

        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="resultsTable")
        if not table:
            if page == 0:
                log.warning("No results table at %s", url)
                # Save page 0 anyway so we know we tried
                (out_dir / "0.html").write_text(html, encoding="utf-8")
            break

        rows = table.find_all("tr")
        data_rows = [r for r in rows if r.find("td")]
        if not data_rows:
            break

        (out_dir / f"{page}.html").write_text(html, encoding="utf-8")
        log.info("  Page %d: %d rows saved", page, len(data_rows))
        page += 1

        if page > 50:
            log.warning("Reached page limit for %s", distance_url)
            break

    return out_dir


def parse_distance_html(distance_id: str) -> list[dict]:
    """Parse results from stored HTML files for a distance."""
    html_dir = _html_dir(distance_id)
    if not html_dir.exists():
        log.warning("No HTML directory for %s", distance_id)
        return []

    results = []
    col_map = {}
    page = 0
    prev_rank = 0
    max_time = 0  # max time_seconds seen so far for ranked results
    spurious = False  # once a rank/time anomaly is detected, all remaining rows are spurious

    while True:
        html_file = html_dir / f"{page}.html"
        if not html_file.exists():
            break

        soup = BeautifulSoup(html_file.read_text(encoding="utf-8"), "html.parser")
        table = soup.find("table", id="resultsTable")
        if not table:
            break

        rows = table.find_all("tr")
        data_rows = [r for r in rows if r.find("td")]
        if not data_rows:
            break

        # Build column index map from header row (once)
        if page == 0:
            header_row = rows[0] if rows else None
            col_map = {}
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
                if not headers:
                    headers = [td.get_text(strip=True) for td in header_row.find_all("td")]
                HEADER_ALIASES = {
                    "Koht": "rank",
                    "N. koht": "rank_cat",
                    "Nr.": "bib",
                    "Aeg": "time",
                    "Netoaeg": "net_time",
                    "Nimi": "name",
                    "VKL": "category",
                    "VKL koht": "cat_rank",
                    "Võiskond": "team",
                }
                for i, h in enumerate(headers):
                    if h in HEADER_ALIASES:
                        col_map[HEADER_ALIASES[h]] = i
            log.debug("Column map: %s", col_map)

        for row in data_rows:
            cells = row.find_all("td")

            def _cell(key):
                idx = col_map.get(key)
                if idx is not None and idx < len(cells):
                    return cells[idx].get_text(strip=True)
                return ""

            rank_text = _cell("rank")
            rank_cat_text = _cell("rank_cat")
            bib = _cell("bib")
            time_raw = _cell("time")
            name = _cell("name")
            category = _cell("category")

            if not bib and not name:
                continue

            rank_overall = int(rank_text) if rank_text.isdigit() else None
            rank_category = int(rank_cat_text) if rank_cat_text.isdigit() else None

            # Detect spurious rows: ranks jump or times suddenly drop
            # (e.g. half-marathon results appended after marathon finishers).
            if not spurious and rank_overall is not None:
                time_seconds_check = _parse_time(time_raw)
                if prev_rank > 0 and rank_overall > prev_rank + 10:
                    spurious = True
                    log.info("  Rank gap %d -> %d: marking remaining ranked rows as DNF",
                             prev_rank, rank_overall)
                elif max_time > 0 and time_seconds_check is not None and time_seconds_check < max_time * 0.5:
                    spurious = True
                    log.info("  Time reversal at rank %d: %s vs slowest %ds: marking remaining as DNF",
                             rank_overall, time_raw, max_time)
                else:
                    prev_rank = rank_overall
                    if time_seconds_check is not None:
                        max_time = max(max_time, time_seconds_check)

            if spurious and rank_overall is not None:
                # Spurious row: zero out time and rank, mark as DNF
                time_seconds = None
                time_raw = ""
                rank_overall = None
                rank_category = None
                dnf = True
            else:
                time_seconds = _parse_time(time_raw)
                dnf = time_seconds is None and rank_overall is None

            gender = _extract_gender(category)
            result_id = f"{distance_id}_{bib}" if bib else f"{distance_id}_row{len(results)}"

            results.append({
                "id": result_id,
                "distance_id": distance_id,
                "rank_overall": rank_overall,
                "rank_category": rank_category,
                "bib": bib,
                "name": name,
                "category": category,
                "gender": gender,
                "time_seconds": time_seconds,
                "time_raw": time_raw,
                "dnf": dnf,
            })

        log.info("  Page %d: %d results parsed", page, len(data_rows))
        page += 1

    return results


def scrape_distance(distance_id: str, distance_url: str) -> list[dict]:
    """Fetch HTML + parse results. Convenience wrapper for full scrape."""
    fetch_distance_html(distance_id, distance_url)
    return parse_distance_html(distance_id)
