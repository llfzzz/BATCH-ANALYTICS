#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
./scripts/bootstrap.sh
exec .venv/bin/python -m uvicorn portfolio_service.app:app --host 127.0.0.1 --port "${PORT:-8767}"
