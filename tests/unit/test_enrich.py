import logging
from pathlib import Path
from typing import Any

import pytest

from clickpipe import enrich
from clickpipe.enrich import country, parse_ua, referrer_domain

IPHONE = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1"
)
PIXEL = (
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Mobile Safari/537.36"
)
IPAD = (
    "Mozilla/5.0 (iPad; CPU OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/18.0 Mobile/15E148 Safari/604.1"
)
ANDROID_TABLET = (
    "Mozilla/5.0 (Linux; Android 14; SM-X710) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)
WINDOWS_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)
MAC_SAFARI = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/18.0 Safari/605.1.15"
)
GOOGLEBOT = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://www.google.com/search?q=x", "google.com"),
        ("https://X.com/", "x.com"),
        ("http://News.Ycombinator.com:8080/item?id=1", "news.ycombinator.com"),
        ("www.linkedin.com/feed", "linkedin.com"),
        ("https://www.reddit.com.", "reddit.com"),
        (None, ""),
        ("", ""),
        ("   ", ""),
        ("http://[::1", "invalid"),
        ("https://exa mple.com/", "invalid"),
        ("::::", "invalid"),
        ("https://", "invalid"),
    ],
)
def test_referrer_domain(url: str | None, expected: str) -> None:
    assert referrer_domain(url) == expected


@pytest.mark.parametrize(
    ("ua", "expected"),
    [
        (WINDOWS_CHROME, ("desktop", "Chrome")),
        (MAC_SAFARI, ("desktop", "Safari")),
        (IPHONE, ("mobile", "Mobile Safari")),
        (PIXEL, ("mobile", "Chrome Mobile")),
        (IPAD, ("tablet", "Mobile Safari")),
        (ANDROID_TABLET, ("tablet", "Chrome")),
        (GOOGLEBOT, ("bot", "Googlebot")),
        ("curl/8.5.0", ("unknown", "curl")),
        ("garbage!!", ("unknown", "unknown")),
        ("", ("unknown", "unknown")),
        (None, ("unknown", "unknown")),
    ],
)
def test_parse_ua(ua: str | None, expected: tuple[str, str]) -> None:
    assert parse_ua(ua) == expected


def test_parse_ua_is_cached() -> None:
    parse_ua.cache_clear()
    parse_ua(WINDOWS_CHROME)
    parse_ua(WINDOWS_CHROME)
    info = parse_ua.cache_info()
    assert (info.hits, info.misses, info.maxsize) == (1, 1, 10_000)


def test_country_falls_back_to_zz_without_database(tmp_path: Path) -> None:
    assert country("8.8.8.8", db_path="") == "ZZ"
    assert country("8.8.8.8", db_path=str(tmp_path / "missing.mmdb")) == "ZZ"
    assert country(None, db_path=str(tmp_path / "missing.mmdb")) == "ZZ"


class FakeReader:
    def get(self, ip: str) -> Any:
        if ip == "garbage":
            raise ValueError(f"'{ip}' does not appear to be an IPv4 or IPv6 address")
        return {"81.2.69.142": {"country": {"iso_code": "GB"}}}.get(ip)


def test_country_uses_database_and_never_logs_ip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    db = tmp_path / "GeoLite2-Country.mmdb"
    db.write_bytes(b"placeholder")  # file must exist; the reader itself is faked
    monkeypatch.setattr(enrich, "_open_reader", lambda path: FakeReader())

    with caplog.at_level(logging.DEBUG, logger="clickpipe.enrich"):
        assert country("81.2.69.142", db_path=str(db)) == "GB"
        assert country("203.0.113.9", db_path=str(db)) == "ZZ"  # not in the database
        assert country("garbage", db_path=str(db)) == "ZZ"  # reader raises
    assert "81.2.69.142" not in caplog.text
    assert "garbage" not in caplog.text
