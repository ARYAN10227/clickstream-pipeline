"""Enrichment of click events: referrer domain, device class, browser and country.

IP addresses are only used for the GeoIP lookup; they are never stored or logged.
"""

import logging
import os
import re
from functools import lru_cache
from typing import Any
from urllib.parse import urlsplit

from ua_parser import parse as parse_user_agent

from clickpipe.config import get_settings

log = logging.getLogger(__name__)

UNKNOWN = "unknown"
UNKNOWN_COUNTRY = "ZZ"
BOT_RE = re.compile(r"bot|crawl|spider|slurp|preview|headless|monitor", re.IGNORECASE)
DESKTOP_OS = frozenset({"Windows", "Mac OS X", "Linux", "Ubuntu", "Fedora", "Chrome OS", "FreeBSD"})


def referrer_domain(url: str | None) -> str:
    """Lowercased host of a referrer URL without a leading "www."; "" if none, "invalid" if bad."""
    if not url or not url.strip():
        return ""
    text = url.strip()
    try:
        parts = urlsplit(text if "//" in text else f"//{text}")
        host = parts.hostname
    except ValueError:
        return "invalid"
    if not host or not re.fullmatch(r"[a-z0-9.-]+|\[?[0-9a-f:.]+\]?", host):
        return "invalid"
    host = host.rstrip(".")
    return host.removeprefix("www.") or "invalid"


@lru_cache(maxsize=10_000)
def parse_ua(user_agent: str | None) -> tuple[str, str]:
    """Return (device, browser): device is desktop/mobile/tablet/bot/unknown."""
    if not user_agent or not user_agent.strip():
        return UNKNOWN, UNKNOWN
    result = parse_user_agent(user_agent)
    browser = result.user_agent.family if result.user_agent else UNKNOWN
    if browser == "Other":
        browser = UNKNOWN
    device_family = result.device.family if result.device else ""
    device_brand = (result.device.brand or "") if result.device else ""
    os_family = result.os.family if result.os else ""

    if device_family == "Spider" or BOT_RE.search(user_agent):
        device = "bot"
    elif (
        "iPad" in device_family
        or "Tablet" in device_family
        or "Tablet" in device_brand
        or "Tablet" in user_agent
        or (os_family == "Android" and "Mobile" not in user_agent)
    ):
        device = "tablet"
    elif os_family in ("iOS", "Android") or "Mobile" in user_agent:
        device = "mobile"
    elif os_family in DESKTOP_OS:
        device = "desktop"
    else:
        device = UNKNOWN
    return device, browser


@lru_cache(maxsize=4)
def _open_reader(path: str) -> Any:
    """Open the GeoIP database once per path (lazily, on first lookup)."""
    import maxminddb

    log.info("opened GeoIP database %s", path)
    return maxminddb.open_database(path)


def country(ip: str | None, db_path: str | None = None) -> str:
    """ISO 3166 country code for an IP via the GeoIP database, else "ZZ".

    db_path defaults to the GEOIP_DB_PATH setting; without a readable file, "ZZ".
    """
    path = db_path if db_path is not None else get_settings().geoip_db_path
    if not ip or not path or not os.path.isfile(path):
        return UNKNOWN_COUNTRY
    try:
        record = _open_reader(path).get(ip)
    except Exception as exc:  # bad IP string or unreadable database: never log the IP
        log.debug("GeoIP lookup failed (%s)", type(exc).__name__)
        return UNKNOWN_COUNTRY
    if not isinstance(record, dict):
        return UNKNOWN_COUNTRY
    code = (record.get("country") or record.get("registered_country") or {}).get("iso_code")
    return code if isinstance(code, str) and len(code) == 2 else UNKNOWN_COUNTRY
