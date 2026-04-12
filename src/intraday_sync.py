"""
intraday_sync.py
盤中每小時快照：09:00 ~ 13:30（台北時間），每整點執行一次。

雲端部署說明：
  - Docker service：直接 `python intraday_sync.py` 跑 schedule loop
  - GCP Cloud Scheduler / AWS EventBridge：只呼叫 run_sync()，不需 schedule loop
"""

import time
import schedule
import yfinance as yf
import requests
import pytz
from datetime import datetime
from database import get_connection, init_db
from sync_tdcc import sync_tdcc_weekly

# ── 常數 ─────────────────────────────────────────────────────
TW_TZ        = pytz.timezone("Asia/Taipei")
MARKET_OPEN  = (9,  0)
MARKET_CLOSE = (13, 30)
BATCH_SIZE   = 200       # yfinance 每批張數
HEADERS      = {"User-Agent": "Mozilla/5.0"}

# ── 交易時間判斷 ──────────────────────────────────────────────

def is_trading_time() -> bool:
    now = datetime.now(TW_TZ)
    if now.weekday() > 4:
        return False
    t = (now.hour, now.minute)
    return MARKET_OPEN <= t <= MARKET_CLOSE

# ── 股票清單（上市 .TW / 上櫃 .TWO）────────────────────────────

def fetch_ticker_map() -> dict[str, str]:
    """
    從 TWSE / TPEx OpenAPI 抓取股票清單，
    回傳 {stock_id: yfinance_suffix}，例如 {"2330": "TW", "6505": "TWO"}
    """
    mapping: dict[str, str] = {}

    # 上市（TWSE）
    try:
        url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        data = requests.get(url, headers=HEADERS, timeout=20).json()
        for item in data:
            sid = str(item.get("Code", "")).strip()
            if sid and len(sid) <= 6 and sid.isdigit():
                mapping[sid] = "TW"
        print(f"✅ 上市 {sum(1 for v in mapping.values() if v == 'TW')} 檔")
    except Exception as e:
        print(f"⚠️ 上市清單抓取失敗: {e}")

    # 上櫃（TPEx）
    try:
        url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_realtime_quotes"
        data = requests.get(url, headers=HEADERS, timeout=20).json()
        for item in data:
            sid = str(item.get("SecuritiesCompanyCode", "")).strip()
            if sid and len(sid) <= 6 and sid.isdigit():
                mapping[sid] = "TWO"
        print(f"✅ 上櫃 {sum(1 for v in mapping.values() if v == 'TWO')} 檔")
    except Exception as e:
        print(f"⚠️ 上櫃清單抓取失敗: {e}")

    return mapping

# ── yfinance 分批抓取 ─────────────────────────────────────────

def fetch_batch(yf_tickers: list[str]) -> dict[str, float]:
    """
    輸入 yfinance ticker 清單（如 ['2330.TW', '6505.TWO']），
    回傳 {ticker: 最新收盤價}
    """
    prices: dict[str, float] = {}

    for i in range(0, len(yf_tickers), BATCH_SIZE):
        chunk = yf_tickers[i : i + BATCH_SIZE]
        try:
            raw = yf.download(
                chunk,
                period="1d",
                interval="1h",
                progress=False,
                auto_adjust=True,
                threads=True,
            )
            if raw.empty:
                continue

            close = raw["Close"] if "Close" in raw.columns else raw.get("close")
            if close is None:
                continue

            # 單檔時 close 是 Series，多檔時是 DataFrame
            if hasattr(close, "columns"):
                last_row = close.iloc[-1]
                for ticker in chunk:
                    val = last_row.get(ticker)
                    if val and not str(val) == "nan":
                        prices[ticker] = float(val)
            else:
                # 只有一檔
                val = close.iloc[-1]
                if val and str(val) != "nan":
                    prices[chunk[0]] = float(val)

        except Exception as e:
            print(f"⚠️ 批次 {i//BATCH_SIZE + 1} 失敗: {e}")

    return prices

# ── 寫入 DB ───────────────────────────────────────────────────

def save_snapshot(snapshot_time: datetime, prices: dict[str, float]) -> int:
    if not prices:
        return 0

    rows = [
        (ticker.split(".")[0], snapshot_time, price)
        for ticker, price in prices.items()
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO twse_intraday (stock_id, snapshot_time, close_price)
                VALUES (%s, %s, %s)
                ON CONFLICT (stock_id, snapshot_time) DO UPDATE
                    SET close_price = EXCLUDED.close_price
            """, rows)
            conn.commit()
    return len(rows)

# ── 主流程 ────────────────────────────────────────────────────

_ticker_map: dict[str, str] = {}   # 快取，避免每次都重抓清單

def run_sync():
    global _ticker_map

    if not is_trading_time():
        print(f"⏸️  {datetime.now(TW_TZ).strftime('%H:%M')} 非交易時間，跳過")
        return

    now_tw = datetime.now(TW_TZ)
    print(f"\n{'='*50}")
    print(f"🕐 {now_tw.strftime('%Y-%m-%d %H:%M')} 開始盤中快照")

    # 每天開盤時重新抓清單（或清單為空時）
    if not _ticker_map or now_tw.hour == 9:
        print("📋 更新股票清單...")
        _ticker_map = fetch_ticker_map()

    if not _ticker_map:
        print("❌ 股票清單為空，放棄")
        return

    yf_tickers = [f"{sid}.{sfx}" for sid, sfx in _ticker_map.items()]
    print(f"📡 抓取 {len(yf_tickers)} 檔行情（分批 {BATCH_SIZE} 檔）...")

    prices = fetch_batch(yf_tickers)
    snapshot_time = now_tw.replace(tzinfo=None)   # 存 naive datetime（本地時間）

    count = save_snapshot(snapshot_time, prices)
    elapsed = (datetime.now(TW_TZ) - now_tw).seconds
    print(f"✅ 完成：{count} 筆，耗時 {elapsed} 秒")

# ── Schedule Loop（Docker / 本機用）─────────────────────────

def run_tdcc_sync():
    print(f"\n{'='*50}")
    print(f"📊 {datetime.now(TW_TZ).strftime('%Y-%m-%d %H:%M')} 開始集保週資料同步")
    try:
        sync_tdcc_weekly()
    except Exception as e:
        print(f"❌ 集保同步失敗: {e}")

def start_scheduler():
    init_db()  # 確保資料表存在
    print("🚀 盤中快照排程啟動（09:00 ~ 13:30 每整點）")
    print("📊 集保週資料排程：每週五 18:00 自動同步")

    # 盤中每整點執行
    for hour in range(9, 14):
        schedule.every().day.at(f"{hour:02d}:00").do(run_sync)

    # 額外加 13:30 收盤快照
    schedule.every().day.at("13:30").do(run_sync)

    # 每週五 18:00 同步集保週資料（收盤後資料已更新）
    schedule.every().friday.at("18:00").do(run_tdcc_sync)

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    start_scheduler()
