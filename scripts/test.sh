#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/bootstrap.sh
source scripts/env.sh
mkdir -p artifacts
exec .venv/bin/python -m pytest -q --junitxml=artifacts/test-results.xml "$@"
