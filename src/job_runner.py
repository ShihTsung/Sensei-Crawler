"""
job_runner.py
供 Cloud Scheduler 呼叫的輕量 Flask 服務。
部署為獨立的 Cloud Run Service（sensei-jobs），
設定 --no-allow-unauthenticated，由 Cloud Scheduler 帶 OIDC token 呼叫。

Cloud Scheduler 設定範例：
  盤中快照：0 1,2,3,4,5 * * 1-5  Asia/UTC  → POST /intraday
  集保週資料：0 10 * * 5          Asia/UTC  → POST /tdcc
  （Asia/Taipei UTC+8：9-13點 = UTC 1-5點；週五18點 = UTC 10點）
"""

import logging

from flask import Flask, jsonify

from intraday_sync import run_sync
from sync_tdcc import sync_tdcc_weekly
from twse_historical_sync import sync_historical

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/intraday", methods=["POST"])
def intraday():
    logger.info("Cloud Scheduler 觸發：盤中快照")
    try:
        run_sync()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("盤中快照失敗: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/tdcc", methods=["POST"])
def tdcc():
    logger.info("Cloud Scheduler 觸發：集保週資料")
    try:
        sync_tdcc_weekly()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("集保同步失敗: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/prices", methods=["POST"])
def prices():
    logger.info("Cloud Scheduler 觸發：每日行情同步")
    try:
        sync_historical()
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("行情同步失敗: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/healthz")
def health():
    return jsonify({"status": "ok"})
