#!/usr/bin/env bash
# 地端啟動 Sensei-Crawler（不使用 Docker，連本機 Homebrew postgresql）
#
#   ./run.sh app        # 啟動 Streamlit 儀表板（預設）
#   ./run.sh intraday   # 啟動盤中快照 schedule loop（背景常駐用）
#   ./run.sh init        # 對資料庫建表（idempotent）
#
# 前置：本機已安裝並啟動 postgresql（brew services start postgresql@17），
#       且已 cp .env.example .env 填好連線資訊。
set -euo pipefail
cd "$(dirname "$0")"

VENV=.venv
if [ ! -x "$VENV/bin/python" ]; then
  echo ">> 建立 venv 並安裝套件..."
  python3.13 -m venv "$VENV"
  "$VENV/bin/pip" install --upgrade pip -q
  "$VENV/bin/pip" install -r requirements.txt
fi

export PYTHONPATH=src
cmd="${1:-app}"
case "$cmd" in
  app)
    exec "$VENV/bin/streamlit" run src/app.py --server.port "${PORT:-8503}" --server.address 127.0.0.1
    ;;
  intraday)
    exec "$VENV/bin/python" src/intraday_sync.py
    ;;
  init)
    exec "$VENV/bin/python" src/database.py
    ;;
  *)
    echo "用法: ./run.sh [app|intraday|init]" >&2; exit 1
    ;;
esac
