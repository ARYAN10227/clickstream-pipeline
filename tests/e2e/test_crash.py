"""End-to-end: SIGKILL the consumer mid-stream; no event is lost or duplicated.

Needs the full stack: run `make up` first, then `make test-e2e`. Kills and restarts the
`consumer` compose service, so run it from the repository root.
"""

from pathlib import Path

import pytest
from helpers import load_tool, require_stack

from clickpipe.config import get_settings

pytestmark = pytest.mark.e2e


def test_kill_and_restart_loses_and_duplicates_nothing(tmp_path: Path) -> None:
    settings = get_settings()
    require_stack(settings).close()
    crash_test = load_tool("crash_test")

    result, checks, report = crash_test.run_crash_test(
        count=60_000,
        rate=20_000,
        kill_after=2,
        restart_after=2,
        timeout_s=180,
        results_dir=tmp_path,
    )

    summary = crash_test.render_text(result, checks)
    assert result.produced == 60_000, summary
    assert result.unique_event_ids == result.produced, f"events lost:\n{summary}"
    assert result.final_count == result.produced, f"duplicates left:\n{summary}"
    assert all(c.passed for c in checks), summary
    assert report.parent == tmp_path
