import random
import re
from collections import Counter

import pytest

from clickpipe.fake import CODE_LEN, MAX_CODE_LEN, make_event
from clickpipe.schema import ClickEvent

NOW_MS = 1700000000123
BASE62_CODE = re.compile(rf"^[0-9a-zA-Z]{{{CODE_LEN}}}$")


def test_generated_events_validate() -> None:
    rng = random.Random(1)
    for _ in range(2000):
        event = ClickEvent.model_validate(make_event(rng, NOW_MS))
        assert event.event_time == event.produced_at == NOW_MS


def test_short_codes_are_six_character_base62() -> None:
    rng = random.Random(4)
    codes = {make_event(rng, NOW_MS)["short_code"] for _ in range(5000)}
    assert all(BASE62_CODE.match(code) for code in codes)
    assert len(codes) > 100  # many distinct links, not a handful


def test_same_seed_gives_same_events() -> None:
    rng_a, rng_b = random.Random(42), random.Random(42)
    a = [make_event(rng_a, NOW_MS) for _ in range(100)]
    b = [make_event(rng_b, NOW_MS) for _ in range(100)]
    assert a == b
    assert len({e["event_id"] for e in a}) == 100


def test_code_pool_depends_only_on_pool_seed() -> None:
    # Different event RNGs (e.g. parallel producer processes) draw from the same links ...
    pool_a = {make_event(random.Random(1), NOW_MS, n_codes=50)["short_code"] for _ in range(1)}
    rng_x, rng_y = random.Random(7), random.Random(8)
    codes_x = {make_event(rng_x, NOW_MS, n_codes=50)["short_code"] for _ in range(3000)}
    codes_y = {make_event(rng_y, NOW_MS, n_codes=50)["short_code"] for _ in range(3000)}
    assert codes_x == codes_y and len(codes_x) == 50
    assert pool_a <= codes_x
    # ... and a different pool_seed gives a different set of links.
    rng_z = random.Random(7)
    codes_z = {
        make_event(rng_z, NOW_MS, n_codes=50, pool_seed=1)["short_code"] for _ in range(3000)
    }
    assert codes_z.isdisjoint(codes_x)


def test_code_prefix_applied_and_length_within_limit() -> None:
    rng = random.Random(7)
    for prefix in ("run42-", "p" * (MAX_CODE_LEN - CODE_LEN)):
        for _ in range(500):
            code = make_event(rng, NOW_MS, code_prefix=prefix, n_codes=100_000)["short_code"]
            assert code.startswith(prefix)
            assert BASE62_CODE.match(code[len(prefix) :])
            assert len(code) <= MAX_CODE_LEN


def test_too_long_prefix_is_rejected() -> None:
    with pytest.raises(ValueError, match="too long"):
        make_event(random.Random(0), NOW_MS, code_prefix="p" * (MAX_CODE_LEN - CODE_LEN + 1))


def test_popularity_is_skewed() -> None:
    rng = random.Random(3)
    counts = Counter(make_event(rng, NOW_MS)["short_code"] for _ in range(20_000))
    top10 = sum(n for _, n in counts.most_common(10))
    assert top10 > 0.25 * 20_000  # 1% of links get well over 1% of clicks
