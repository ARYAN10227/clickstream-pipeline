import random
from collections import Counter

import pytest

from clickpipe.fake import make_event
from clickpipe.schema import ClickEvent

NOW_MS = 1700000000123


def test_generated_events_validate() -> None:
    rng = random.Random(1)
    for _ in range(2000):
        event = ClickEvent.model_validate(make_event(rng, NOW_MS))
        assert event.event_time == event.produced_at == NOW_MS


def test_same_seed_gives_same_events() -> None:
    rng_a, rng_b = random.Random(42), random.Random(42)
    a = [make_event(rng_a, NOW_MS) for _ in range(100)]
    b = [make_event(rng_b, NOW_MS) for _ in range(100)]
    assert a == b
    assert len({e["event_id"] for e in a}) == 100


def test_code_prefix_applied_and_length_within_limit() -> None:
    rng = random.Random(7)
    prefix = "run42-"
    for _ in range(1000):
        code = make_event(rng, NOW_MS, code_prefix=prefix, n_codes=100_000)["short_code"]
        assert code.startswith(prefix)
        assert len(code) <= 16


def test_too_long_prefix_is_rejected() -> None:
    with pytest.raises(ValueError, match="too long"):
        make_event(random.Random(0), NOW_MS, code_prefix="p" * 15, n_codes=1000)


def test_popularity_is_skewed() -> None:
    rng = random.Random(3)
    counts = Counter(make_event(rng, NOW_MS)["short_code"] for _ in range(20_000))
    top10 = sum(n for _, n in counts.most_common(10))
    assert top10 > 0.25 * 20_000  # 1% of links get well over 1% of clicks
