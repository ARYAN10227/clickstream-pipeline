import importlib.util
import sys
from collections import Counter
from pathlib import Path

import orjson
import pytest

from clickpipe.schema import DlqReason, Invalid, Valid, validate

TOOL = Path(__file__).resolve().parents[2] / "tools" / "replay.py"
spec = importlib.util.spec_from_file_location("replay", TOOL)
assert spec is not None and spec.loader is not None
replay = importlib.util.module_from_spec(spec)
sys.modules["replay"] = replay
spec.loader.exec_module(replay)

NOW_MS = 1_700_000_000_000
N = 1000


@pytest.fixture(scope="module")
def plan():  # type: ignore[no-untyped-def]
    return replay.build_plan(N, 0.05, 0.01, 0.01, seed=42, prefix="rabcde", now_ms=NOW_MS)


def test_counts_add_up(plan) -> None:  # type: ignore[no-untyped-def]
    c = plan.counts
    kinds = Counter(item.kind for item in plan.items)
    expected_total = (
        c["n_unique_valid"] + c["duplicates_sent"] + c["late"] + sum(c["bad_by_reason"].values())
    )
    assert c["produced_total"] == expected_total == len(plan.items)
    assert kinds["valid"] == N
    assert kinds["dup"] == c["duplicates_sent"] == 50
    assert kinds["late"] == c["late"] == 10
    assert c["bad_by_reason"] == {"bad_json": 4, "missing_field": 3, "schema_invalid": 3}
    assert {k: kinds[k] for k in replay.BAD_REASONS} == c["bad_by_reason"]


def test_duplicates_copy_an_earlier_valid_event(plan) -> None:  # type: ignore[no-untyped-def]
    for index, item in enumerate(plan.items):
        if item.kind != "dup":
            continue
        original = plan.items[item.dup_of]
        assert item.dup_of < index
        assert original.kind == "valid"
        assert item.value == original.value and item.key == original.key


def test_unique_event_ids_among_valid_events(plan) -> None:  # type: ignore[no-untyped-def]
    ids = [orjson.loads(i.value)["event_id"] for i in plan.items if i.kind in ("valid", "dup")]
    assert len(set(ids)) == N
    late_ids = {orjson.loads(i.value)["event_id"] for i in plan.items if i.kind == "late"}
    assert len(late_ids) == 10 and not late_ids & set(ids)


def test_records_get_the_expected_validation_result(plan) -> None:  # type: ignore[no-untyped-def]
    expected = {
        "valid": None,
        "dup": None,
        "late": DlqReason.LATE_EVENT,
        "bad_json": DlqReason.BAD_JSON,
        "missing_field": DlqReason.MISSING_FIELD,
        "schema_invalid": DlqReason.SCHEMA_INVALID,
    }
    for item in plan.items:
        result = validate(item.value, NOW_MS, late_threshold_s=300, future_skew_s=60)
        if expected[item.kind] is None:
            assert isinstance(result, Valid), item
            assert result.event.short_code.startswith("rabcde")
        else:
            assert isinstance(result, Invalid) and result.reason is expected[item.kind], item


def test_same_seed_same_plan(plan) -> None:  # type: ignore[no-untyped-def]
    again = replay.build_plan(N, 0.05, 0.01, 0.01, seed=42, prefix="rabcde", now_ms=NOW_MS)
    other = replay.build_plan(N, 0.05, 0.01, 0.01, seed=43, prefix="rabcde", now_ms=NOW_MS)
    assert again.items == plan.items and again.counts == plan.counts
    assert other.items != plan.items


def test_some_duplicates_soon_and_some_much_later(plan) -> None:  # type: ignore[no-untyped-def]
    gaps = [i - item.dup_of for i, item in enumerate(plan.items) if item.kind == "dup"]
    assert min(gaps) <= replay.DUP_SOON_WINDOW + 5
    assert max(gaps) > 200


def test_prefix_is_derived_from_run_id() -> None:
    assert replay.make_prefix("20261020T001500") == replay.make_prefix("20261020T001500")
    assert replay.make_prefix("20261020T001500") != replay.make_prefix("20261020T001501")
    assert len(replay.make_prefix("x")) == 6 and replay.make_prefix("x").startswith("r")
