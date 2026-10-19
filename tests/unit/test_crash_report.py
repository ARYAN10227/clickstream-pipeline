import importlib.util
import sys
from pathlib import Path

TOOL = Path(__file__).resolve().parents[2] / "tools" / "crash_test.py"
spec = importlib.util.spec_from_file_location("crash_test", TOOL)
assert spec is not None and spec.loader is not None
crash_test = importlib.util.module_from_spec(spec)
sys.modules["crash_test"] = crash_test
spec.loader.exec_module(crash_test)


def _result(**overrides: object) -> object:
    values: dict[str, object] = {
        "run_id": "crash-20261020T001500",
        "prefix": "kabcde",
        "produced": 300_000,
        "delivery_errors": 0,
        "raw_count": 312_345,
        "final_count": 300_000,
        "unique_event_ids": 300_000,
        "killed_at": "2026-10-20T00:15:05.000+00:00",
        "restarted_at": "2026-10-20T00:15:08.000+00:00",
        "lag_at_end": 0,
    }
    return crash_test.CrashResult(**(values | overrides))


def test_all_good_passes() -> None:
    checks = crash_test.build_checks(_result())
    assert [c.passed for c in checks] == [True, True]
    assert crash_test.exit_code(checks) == 0


def test_lost_events_fail_zero_loss() -> None:
    checks = crash_test.build_checks(_result(unique_event_ids=299_990, final_count=299_990))
    assert [c.passed for c in checks] == [False, False]
    assert crash_test.exit_code(checks) == 1


def test_uncollapsed_duplicates_fail_zero_duplicates_only() -> None:
    checks = crash_test.build_checks(_result(final_count=300_007))
    assert [c.passed for c in checks] == [True, False]
    assert crash_test.exit_code(checks) == 1


def test_report_shows_checks_timestamps_and_collapsed_duplicates() -> None:
    result = _result()
    checks = crash_test.build_checks(result)
    text = crash_test.render_text(result, checks)
    markdown = crash_test.render_markdown(result, checks)
    for report in (text, markdown):
        for check in checks:
            assert check.name in report
        assert "2026-10-20T00:15:05.000+00:00" in report
        assert "2026-10-20T00:15:08.000+00:00" in report
        assert "12,345" in report  # raw - FINAL
    assert "2/2 checks passed" in text
    assert "**2/2 checks passed**" in markdown
