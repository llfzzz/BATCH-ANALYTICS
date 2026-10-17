#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
./scripts/bootstrap.sh
source scripts/env.sh
if [[ ! -d data/input ]]; then .venv/bin/python -m src.generate; fi
mkdir -p artifacts
.venv/bin/python -m src.pipeline "$@"
