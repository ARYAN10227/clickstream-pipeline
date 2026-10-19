"""End-to-end: a replay with duplicates, bad and late records verifies cleanly.

Needs the full stack: run `make up` first, then `make test-e2e`.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from helpers import load_tool, require_stack

from clickpipe.config import get_settings

pytestmark = pytest.mark.e2e


def test_replay_is_deduplicated_and_dlq_routed(tmp_path: Path) -> None:
    settings = get_settings()
    require_stack(settings).close()
    replay, verify = load_tool("replay"), load_tool("verify")
    run_id = "e2e-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")

    result = replay.run_replay(
        n=20_000, dup=0.05, bad=0.01, late=0.01, seed=7, run_id=run_id, results_dir=tmp_path
    )
    assert result.delivery_errors == 0
    assert result.manifest_path.parent == tmp_path

    report = verify.verify(result.manifest, timeout_s=120, results_dir=tmp_path)

    table = verify.render_table(report.checks, report.info)
    assert report.passed, f"verification failed:\n{table}"
    assert len(report.checks) == 7
    assert report.report_path.parent == tmp_path
