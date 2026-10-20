#!/usr/bin/env bash
# One-time setup for a Codespace: install uv, Python deps and a local .env.
set -euo pipefail
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv sync --frozen
[ -f .env ] || cp .env.example .env
cat <<'MSG'

ClickPipe is ready. The stack starts automatically (make up).
Then send traffic:   uv run python tools/loadgen.py --rate 2000 --duration 120
Grafana opens on port 3000 (login admin / change_me).
MSG
