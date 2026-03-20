"""Shared HTTP helpers with rate limiting."""

import time
import logging

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


def fetch(url: str, delay: float = 0.5) -> requests.Response:
    """GET a URL with rate limiting."""
    time.sleep(delay)
    log.debug("GET %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp


def fetch_soup(url: str, delay: float = 0.5) -> BeautifulSoup:
    """GET a URL and return parsed HTML."""
    return BeautifulSoup(fetch(url, delay).text, "html.parser")
