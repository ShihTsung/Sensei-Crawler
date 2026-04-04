"""
sync_top10.py
從 MOPS 抓取前十大股東資料（每季更新）。
每支股票需個別請求，1700+ 檔約需 30~60 分鐘，建議手動觸發後背景等待。
"""

import requests
import time
import re
from io import StringIO
from datetime import datetime
import pandas as pd
from database import get_connection

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://mops.twse.com.tw/",
}
MOPS_URL = "https://mops.twse.com.tw/mops/web/ajax_t10st03"


def get_all_stock_ids() -> list[tuple[str, str]]:
    """從 DB 取得所有股票代碼與市場類型，回傳 [(stock_id, 'sii'|'otc'), ...]"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 優先從 company_info 取市場類型
            cur.execute("""
                SELECT stock_id, market_type FROM company_info
                WHERE market_type IN ('上市', '上櫃')
                ORDER BY stock_id
            """)
            rows = cur.fetchall()
            if rows:
                return [
                    (r[0], "sii" if r[1] == "上市" else "otc")
                    for r in rows
                ]
            # fallback：全用上市
            cur.execute("SELECT DISTINCT stock_id FROM twse_prices ORDER BY stock_id")
            return [(r[0], "sii") for r in cur.fetchall()]


def _roc_year_season(year: int, season: int) -> tuple[int, int]:
    """西元年轉民國年"""
    return year - 1911, season


def fetch_top10(stock_id: str, typek: str, roc_year: int, season: int) -> list[dict]:
    """抓取單一股票的前十大股東，回傳 list of dict 或空 list"""
    try:
        resp = requests.post(
            MOPS_URL,
            data={
                "step": "1", "firstin": "1", "off": "1",
                "TYPEK": typek,
                "year": str(roc_year),
                "season": str(season),
                "co_id": stock_id,
            },
            headers=HEADERS,
            timeout=20,
        )
        resp.encoding = "utf-8"

        if "查詢無資料" in resp.text or "無此資料" in resp.text:
            return []

        dfs = pd.read_html(StringIO(resp.text))
        df = next(
            (t for t in dfs if any("股東" in str(c) or "持股" in str(c) for c in t.columns)),
            None,
        )
        if df is None:
            return []

        # 標準化欄位名稱
        df.columns = [str(c).strip() for c in df.columns]
        name_col  = next((c for c in df.columns if "股東" in c or "姓名" in c or "名稱" in c), None)
        share_col = next((c for c in df.columns if "持股" in c and "比例" not in c and "%" not in c), None)
        rate_col  = next((c for c in df.columns if "比例" in c or "%" in c), None)

        if not name_col:
            return []

        results = []
        for rank, (_, row) in enumerate(df.iterrows(), start=1):
            name = str(row.get(name_col, "")).strip()
            if not name or name in ("合計", "總計", "nan"):
                continue

            def _clean_num(val):
                try:
                    return float(re.sub(r"[^\d.]", "", str(val)))
                except Exception:
                    return None

            results.append({
                "rank":        rank,
                "name":        name,
                "held_shares": _clean_num(row.get(share_col)) if share_col else None,
                "held_rate":   _clean_num(row.get(rate_col))  if rate_col  else None,
            })
        return results

    except Exception:
        return []


def sync_top10(year: int, season: int,
               progress_cb=None) -> int:
    """
    抓取指定年季的前十大股東。
    progress_cb(done, total) 可選，供 UI 顯示進度。
    回傳寫入筆數。
    """
    roc_year, s = _roc_year_season(year, season)
    year_period = f"{year}Q{season}"
    stocks = get_all_stock_ids()
    total_stocks = len(stocks)
    written = 0

    print(f"📋 共 {total_stocks} 支股票，年季：{year_period}")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS twse_top10_shareholders (
                    stock_id    VARCHAR(10),
                    year_period VARCHAR(20),
                    rank        INT,
                    name        VARCHAR(100),
                    held_shares BIGINT,
                    held_rate   NUMERIC,
                    PRIMARY KEY (stock_id, year_period, name)
                );
            """)

            for i, (sid, typek) in enumerate(stocks):
                rows = fetch_top10(sid, typek, roc_year, s)
                for r in rows:
                    try:
                        cur.execute("""
                            INSERT INTO twse_top10_shareholders
                                (stock_id, year_period, rank, name, held_shares, held_rate)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (stock_id, year_period, name) DO UPDATE SET
                                rank        = EXCLUDED.rank,
                                held_shares = EXCLUDED.held_shares,
                                held_rate   = EXCLUDED.held_rate
                        """, (sid, year_period, r["rank"], r["name"],
                              r["held_shares"], r["held_rate"]))
                        written += 1
                    except Exception:
                        pass

                # 每 50 支 commit 一次，減少鎖定時間
                if (i + 1) % 50 == 0:
                    conn.commit()
                    print(f"  進度 {i+1}/{total_stocks}，已寫入 {written} 筆")

                if progress_cb:
                    progress_cb(i + 1, total_stocks)

                time.sleep(1.5)   # MOPS 頻率保護

            conn.commit()

    print(f"🎉 前十大股東匯入完成：{written} 筆")
    return written


if __name__ == "__main__":
    now = datetime.now()
    season = (now.month - 1) // 3 + 1
    sync_top10(now.year, season)
