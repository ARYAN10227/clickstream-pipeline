"""Deterministic fake click events for load generation and tests."""

import random
import re
import uuid
from functools import lru_cache
from typing import Any

MAX_CODE_LEN = 16
CODE_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
CODE_PREFIX_RE = re.compile(r"^[A-Za-z0-9_-]*$")
ZIPF_EXPONENT = 1.1  # popularity skew: rank 1 is ~2x rank 2, ~10x rank 8

USER_AGENTS: tuple[str, ...] = (
    # desktop
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
    # mobile
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Mobile Safari/537.36",
    # tablet
    "Mozilla/5.0 (iPad; CPU OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/18.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-X710) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36",
    # bot
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
)
USER_AGENT_WEIGHTS: tuple[int, ...] = (20, 12, 8, 25, 20, 5, 3, 4, 3)

REFERRERS: tuple[str | None, ...] = (
    None,  # direct
    "https://www.google.com/",
    "https://x.com/",
    "https://www.linkedin.com/",
    "https://www.reddit.com/",
)
REFERRER_WEIGHTS: tuple[int, ...] = (35, 30, 15, 10, 10)

# First octets that are private, loopback, multicast or otherwise reserved.
NON_PUBLIC_FIRST_OCTETS = frozenset({0, 10, 100, 127, 169, 172, 192, 198, 203, *range(224, 256)})
PUBLIC_FIRST_OCTETS: tuple[int, ...] = tuple(
    o for o in range(1, 224) if o not in NON_PUBLIC_FIRST_OCTETS
)


def _encode(index: int) -> str:
    """Encode a non-negative integer as a base62 string."""
    digits = []
    while True:
        index, rem = divmod(index, len(CODE_CHARS))
        digits.append(CODE_CHARS[rem])
        if index == 0:
            return "".join(reversed(digits))


@lru_cache(maxsize=32)
def _code_pool(n_codes: int) -> tuple[tuple[str, ...], tuple[float, ...]]:
    """Return (codes, cumulative Zipf weights) for a pool of n_codes links."""
    if n_codes < 1:
        raise ValueError("n_codes must be >= 1")
    codes = tuple(_encode(i) for i in range(n_codes))
    cum: list[float] = []
    total = 0.0
    for rank in range(1, n_codes + 1):
        total += 1.0 / rank**ZIPF_EXPONENT
        cum.append(total)
    return codes, tuple(cum)


def _public_ipv4(rng: random.Random) -> str:
    """Return a random IPv4 address outside the common private and reserved ranges."""
    return (
        f"{rng.choice(PUBLIC_FIRST_OCTETS)}.{rng.randrange(256)}."
        f"{rng.randrange(256)}.{rng.randrange(1, 255)}"
    )


def make_event(
    rng: random.Random, now_ms: int, code_prefix: str = "", n_codes: int = 1000
) -> dict[str, Any]:
    """Return one valid ClickEvent dict; output depends only on rng state and arguments."""
    codes, cum_weights = _code_pool(n_codes)
    if not CODE_PREFIX_RE.match(code_prefix):
        raise ValueError(f"code_prefix {code_prefix!r} may only contain A-Z a-z 0-9 _ -")
    if len(code_prefix) + len(codes[-1]) > MAX_CODE_LEN:
        raise ValueError(
            f"code_prefix {code_prefix!r} is too long for {n_codes} codes "
            f"(short_code would exceed {MAX_CODE_LEN} characters)"
        )
    code = rng.choices(codes, cum_weights=cum_weights)[0]
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "short_code": code_prefix + code,
        "event_time": now_ms,
        "produced_at": now_ms,
        "ip": _public_ipv4(rng),
        "user_agent": rng.choices(USER_AGENTS, weights=USER_AGENT_WEIGHTS)[0],
        "referrer": rng.choices(REFERRERS, weights=REFERRER_WEIGHTS)[0],
        "schema_version": 1,
    }
