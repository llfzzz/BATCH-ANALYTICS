#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -x .venv/bin/python ]]; then
  command -v uv >/dev/null || { echo '需要 uv： https://docs.astral.sh/uv/getting-started/installation/' >&2; exit 1; }
  uv venv --python 3.12 .venv
fi
# Check every pinned distribution; repair a partial environment without network on healthy reruns.
if ! .venv/bin/python - >/dev/null 2>&1 <<'PY'
import importlib.metadata as metadata
import re
from pathlib import Path
import sys
assert sys.version_info[:2] == (3, 12)
for line in Path('requirements.lock').read_text().splitlines():
    match = re.match(r'^([A-Za-z0-9_.-]+)(?:\[[^]]+\])?==([^ ;]+)', line)
    if match:
        assert metadata.version(match[1]) == match[2], match[1]
import fastapi, uvicorn, pyspark, numpy, pytest, httpx
PY
then
  printf '%s\n' '首次安装或修复不完整环境（使用 requirements.lock）…'
  command -v uv >/dev/null || { echo '需要 uv 修复不完整环境。' >&2; exit 1; }
  uv pip sync --python .venv/bin/python requirements.lock
fi
