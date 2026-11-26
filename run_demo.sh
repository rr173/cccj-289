#!/usr/bin/env bash
# Bootstrap the local dependency directory once, then run the demo.
#
# Dependencies live in ./.pylibs (installed with pip --target) because this
# environment's system Python is externally managed.  Nothing is installed
# system-wide and no virtualenv is required.
set -euo pipefail
cd "$(dirname "$0")"

if ! PYTHONPATH="$PWD/.pylibs" python3 -c "import fastapi, uvicorn, httpx" >/dev/null 2>&1; then
  echo "Installing Python dependencies into ./.pylibs ..."
  mkdir -p .pylibs
  if ! PYTHONPATH="$PWD/.pylibs" python3 -m pip --version >/dev/null 2>&1; then
    curl -fsS -o /tmp/get-pip.py https://bootstrap.pypa.io/get-pip.py
    python3 -I /tmp/get-pip.py --target "$PWD/.pylibs" --break-system-packages >/dev/null
  fi
  PYTHONPATH="$PWD/.pylibs" python3 -m pip install --target "$PWD/.pylibs" \
    --upgrade fastapi uvicorn httpx 'pydantic>=2'
fi

exec env PYTHONPATH="$PWD/.pylibs" python3 demo.py "$@"
