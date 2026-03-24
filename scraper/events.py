import re
import logging
from datetime import date

from bs4 import BeautifulSoup

from scraper.http import fetch_soup

BASE = "https://www.sportos.eu"
RESULTS_LIST = f"{BASE}/ee/et/tulemused"

log = logging.getLogger(__name__)


def _extract_date_from_soup(soup: BeautifulSoup) -> date | None:
    """Extract date from og:description meta tag."""
    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", og["content"])
        if m:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    return None


def _has_results_tab(soup: BeautifulSoup, slug: str) -> bool:
    """Check if the tulemused page has a 'Tulemused' tab (meaning results exist)."""
    for a in soup.find_all("a", href=True):
        if a.get_text(strip=True) == "Tulemused" and f"{slug}/tulemused" in a["href"]:
            return True
    return False


def _get_event_date_and_has_results(slug: str) -> tuple[date | None, bool]:
    """Fetch event tulemused page, check for results tab, and extract date.

    Returns (date, has_results). Fetches main page as fallback for date.
    """
    try:
        tulemused_soup = fetch_soup(f"{BASE}/ee/et/{slug}/tulemused/")
        has_results = _has_results_tab(tulemused_soup, slug)
        if not has_results:
            return None, False

        # Try date from tulemused page first
        event_date = _extract_date_from_soup(tulemused_soup)
        if event_date:
            return event_date, True

        # Fall back to main event page for date
        main_soup = fetch_soup(f"{BASE}/ee/et/{slug}/")
        event_date = _extract_date_from_soup(main_soup)
        return event_date, True
    except Exception as e:
        log.warning("Could not check %s: %s", slug, e)
        return None, False


def discover_events(since_date: date, existing_ids: set[str],
                     limit: int | None = None, stop_on_existing: bool = False) -> list[dict]:
    """Discover new running events from the listing pages.

    Returns a list of event dicts. Skips already-scraped events.
    Stops when events are older than since_date.
    """
    events = []
    page = 0
    empty_pages = 0

    while True:
        url = f"{RESULTS_LIST}?page={page}"
        log.info("Discovering events page %d", page)
        soup = fetch_soup(url)

        boxes = soup.select("div.listBox")
        if not boxes:
            log.info("No more events on page %d", page)
            break

        found_running_on_page = False
        stop = False

        for box in boxes:
            sport_img = box.select_one("div.area img")
            if not sport_img or sport_img.get("alt") != "Jooksmine":
                continue

            found_running_on_page = True

            link = box.select_one("div.competition a")
            if not link:
                continue
            href = link.get("href", "")
            slug_m = re.search(r"/ee/et/([^/]+)/?$", href)
            if not slug_m:
                continue
            slug = slug_m.group(1)
            name = link.get_text(strip=True)

            # Skip already-scraped events (no HTTP needed)
            if slug in existing_ids:
                if stop_on_existing:
                    log.info("Hit existing event %s, stopping (incremental mode)", slug)
                    stop = True
                    break
                log.debug("Already scraped: %s", slug)
                continue

            # Check for results and get date
            event_date, has_results = _get_event_date_and_has_results(slug)
            if not has_results:
                log.debug("Skipping %s (no results)", slug)
                continue
            if event_date is None:
                log.warning("Skipping %s (no date found)", slug)
                continue

            if event_date < since_date:
                log.info("Event %s (%s) is before %s, stopping", slug, event_date, since_date)
                stop = True
                break

            events.append({
                "id": slug,
                "name": name,
                "date": event_date,
                "url": f"{BASE}/ee/et/{slug}/",
            })
            existing_ids.add(slug)

            if limit and len(events) >= limit:
                log.info("Reached limit of %d events", limit)
                stop = True
                break

        if stop:
            break

        if found_running_on_page:
            empty_pages = 0
        else:
            empty_pages += 1
            if empty_pages >= 3:
                log.info("No running events for %d consecutive pages, stopping", empty_pages)
                break

        page += 1
        if page > 500:
            log.warning("Reached page limit")
            break

    log.info("Discovered %d new events", len(events))
    return events


def _parse_distance_map_from_event_page(slug: str) -> dict[str, int | None]:
    """Parse the Distantsid section on the main event page.

    Returns a dict mapping distance label -> distance in meters.
    E.g. {"Kadrijooks": 4000, "5. Tondiraba Sisekümme": 10000}
    """
    soup = fetch_soup(f"{BASE}/ee/et/{slug}/")
    dist_map = {}
    for sub in soup.select('div[itemprop="subEvent"]'):
        name_span = sub.select_one('span[itemprop="name"]')
        if not name_span:
            continue
        full_name = name_span.get_text(strip=True)
        distance_m = _parse_distance_meters(full_name)
        # The tab label is the part before ", X km" / ", X min"
        short_label = re.sub(r",\s*[\d.,]+\s*(km|min|m)\s*$", "", full_name).strip()
        dist_map[full_name] = distance_m
        if short_label != full_name:
            dist_map[short_label] = distance_m
    return dist_map


def get_distances(event_id: str) -> list[dict]:
    """Get all distances for an event from its results page."""
    # Build label -> meters map from the main event page's Distantsid section
    dist_map = _parse_distance_map_from_event_page(event_id)

    url = f"{BASE}/ee/et/{event_id}/tulemused/"
    soup = fetch_soup(url)

    distances = []
    seen_codes = set()

    # Find distance tab links on the results page
    for link in soup.find_all("a", href=True):
        href = link["href"]
        m = re.search(rf"/ee/et/{re.escape(event_id)}/tulemused/([^/?]*)", href)
        if m is None:
            continue

        code = m.group(1).rstrip("/")
        label = link.get_text(strip=True)

        # Skip navigation links
        if label in ("Tulemused", "Üldinfo", "") or not label:
            continue

        if code in seen_codes:
            continue
        seen_codes.add(code)

        # Look up distance from event page first, fall back to parsing tab label
        distance_m = dist_map.get(label) or _parse_distance_meters(label)
        dist_url = f"{BASE}/ee/et/{event_id}/tulemused/{code}/" if code else f"{BASE}/ee/et/{event_id}/tulemused/"
        dist_id = f"{event_id}_{code}" if code else f"{event_id}_default"

        distances.append({
            "id": dist_id,
            "event_id": event_id,
            "code": code or "default",
            "label": label,
            "distance_m": distance_m,
            "url": dist_url,
        })
        log.info("  Distance: %s (%sm) -> %s", label, distance_m, code or "default")

    # If no distance tabs found, the event has a single default distance
    if not distances:
        table = soup.find("table", id="resultsTable")
        if table:
            # Try to get distance from event page (single-distance events)
            distance_m = next(iter(dist_map.values()), None) if dist_map else None
            label = next((k for k in dist_map), "default") if dist_map else "default"
            distances.append({
                "id": f"{event_id}_default",
                "event_id": event_id,
                "code": "default",
                "label": label,
                "distance_m": distance_m,
                "url": url,
            })
            log.info("  Single distance: %s (%sm) for %s", label, distance_m, event_id)

    return distances


def _parse_distance_meters(label: str) -> int | None:
    """Parse distance label like '21.1 km', '5 km', '42.2 km' to meters."""
    m = re.search(r"([\d.,]+)\s*km", label, re.IGNORECASE)
    if m:
        try:
            km = float(m.group(1).replace(",", "."))
            return int(km * 1000)
        except ValueError:
            pass
    m = re.search(r"([\d.,]+)\s*m(?:eet|$)", label, re.IGNORECASE)
    if m:
        try:
            return int(float(m.group(1).replace(",", ".")))
        except ValueError:
            pass
    return None
