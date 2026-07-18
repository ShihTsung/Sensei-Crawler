#!/usr/bin/env bash
# 地端啟動 HolderScope（不使用 Docker，連本機 Homebrew postgresql）
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
    # ADDR 預設 0.0.0.0(區網其他電腦可連);要鎖回只限本機就用 ADDR=127.0.0.1 ./run.sh app
    # --server.headless true: 背景/服務執行時不跳 email 提示、不自動開瀏覽器
    exec "$VENV/bin/streamlit" run src/app.py --server.port "${PORT:-8503}" --server.address "${ADDR:-0.0.0.0}" --server.headless true
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
