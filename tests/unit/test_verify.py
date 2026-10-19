import importlib.util
import sys
from pathlib import Path
from typing import Any

TOOL = Path(__file__).resolve().parents[2] / "tools" / "verify.py"
spec = importlib.util.spec_from_file_location("verify", TOOL)
assert spec is not None and spec.loader is not None
verify = importlib.util.module_from_spec(spec)
sys.modules["verify"] = verify
spec.loader.exec_module(verify)

MANIFEST: dict[str, Any] = {
    "run_id": "20261020T001500",
    "prefix": "rabcde",
    "seed": 42,
    "n_unique_valid": 1000,
    "duplicates_sent": 50,
    "bad_by_reason": {"bad_json": 4, "missing_field": 3, "schema_invalid": 3},
    "late": 10,
    "produced_total": 1070,
    "started_at": "2026-10-20T00:15:00+00:00",
    "finished_at": "2026-10-20T00:15:01+00:00",
}
GOOD_DLQ = {"bad_json": 4, "missing_field": 3, "schema_invalid": 3, "late_event": 10}


def _actual(**overrides: Any) -> Any:
    values = {"raw_count": 1012, "final_count": 1000, "unique_event_ids": 1000}
    values |= overrides
    dlq = values.pop("dlq_by_reason", GOOD_DLQ)
    return verify.Actual(dlq_by_reason=dlq, **values)


def test_all_match_passes() -> None:
    checks = verify.build_checks(MANIFEST, _actual())
    assert len(checks) == 7
    assert all(c.passed for c in checks)
    assert verify.exit_code(checks) == 0


def test_one_mismatch_fails() -> None:
    checks = verify.build_checks(MANIFEST, _actual(final_count=999))
    failed = [c.name for c in checks if not c.passed]
    assert failed == ["count() FINAL == n_unique_valid"]
    assert verify.exit_code(checks) == 1


def test_dlq_reason_mismatch_fails_reason_and_total() -> None:
    dlq = GOOD_DLQ | {"late_event": 9}
    checks = verify.build_checks(MANIFEST, _actual(dlq_by_reason=dlq))
    assert [c.name for c in checks if not c.passed] == ["DLQ total == bad + late", "DLQ late_event"]
    assert verify.exit_code(checks) == 1


def test_info_reports_unmerged_duplicates_and_unexpected_reasons() -> None:
    info = dict(verify.info_rows(_actual(dlq_by_reason=GOOD_DLQ | {"future_event": 2})))
    assert info["duplicates not yet merged (raw - FINAL)"] == 12
    assert info["DLQ other reason: future_event"] == 2


def test_table_includes_every_check_and_result() -> None:
    actual = _actual(unique_event_ids=998)
    checks = verify.build_checks(MANIFEST, actual)
    table = verify.render_table(checks, verify.info_rows(actual))
    for check in checks:
        assert check.name in table
    assert table.count("PASS") == 6 and table.count("FAIL") == 1
    assert "6/7 checks passed" in table
    assert "duplicates not yet merged" in table

    markdown = verify.render_markdown(MANIFEST, checks, verify.info_rows(actual), "now")
    assert all(f"| {c.name} |" in markdown for c in checks)
    assert "**6/7 checks passed**" in markdown
