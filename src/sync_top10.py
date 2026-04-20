"""
sync_top10.py
從 MOPS 抓取前十大股東資料（每季更新）。
每支股票需個別請求，1700+ 檔約需 30~60 分鐘，建議由排程觸發。
"""

import logging
import re
import time
from datetime import datetime
from io import StringIO

import pandas as pd
import requests

from database import get_connection, init_db
from http_utils import with_retry

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://mops.twse.com.tw/",
}
MOPS_URL = "https://mops.twse.com.tw/mops/web/ajax_t10st03"


def get_all_stock_ids() -> list[tuple[str, str]]:
    """從 DB 取得所有股票代碼與市場類型，回傳 [(stock_id, 'sii'|'otc'), ...]"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT stock_id, market_type FROM company_info
                    WHERE market_type IN ('上市', '上櫃')
                    ORDER BY stock_id
                """)
                rows = cur.fetchall()
                if rows:
                    return [(r[0], "sii" if r[1] == "上市" else "otc") for r in rows]
            except Exception as e:
                logger.warning("company_info 查詢失敗，使用 fallback: %s", e)
                conn.rollback()

            cur.execute("""
                SELECT DISTINCT stock_id FROM twse_prices
                WHERE stock_id ~ '^[0-9]+$' AND LENGTH(stock_id) BETWEEN 4 AND 6
                ORDER BY stock_id
            """)
            return [(r[0], "sii") for r in cur.fetchall()]


def _roc_year_season(year: int, season: int) -> tuple[int, int]:
    return year - 1911, season


@with_retry()
def _post_mops(stock_id: str, typek: str, roc_year: int, season: int) -> requests.Response:
    return requests.post(
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


def fetch_top10(stock_id: str, typek: str, roc_year: int, season: int) -> list[dict]:
    try:
        resp = _post_mops(stock_id, typek, roc_year, season)
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

        df.columns = [str(c).strip() for c in df.columns]
        name_col  = next((c for c in df.columns if "股東" in c or "姓名" in c or "名稱" in c), None)
        share_col = next((c for c in df.columns if "持股" in c and "比例" not in c and "%" not in c), None)
        rate_col  = next((c for c in df.columns if "比例" in c or "%" in c), None)

        if not name_col:
            return []

        def _clean_num(val):
            try:
                return float(re.sub(r"[^\d.]", "", str(val)))
            except Exception:
                return None

        results = []
        for rank, (_, row) in enumerate(df.iterrows(), start=1):
            name = str(row.get(name_col, "")).strip()
            if not name or name in ("合計", "總計", "nan"):
                continue
            results.append({
                "rank":        rank,
                "name":        name,
                "held_shares": _clean_num(row.get(share_col)) if share_col else None,
                "held_rate":   _clean_num(row.get(rate_col))  if rate_col  else None,
            })
        return results

    except Exception as e:
        logger.warning("fetch_top10 %s 失敗: %s", stock_id, e)
        return []


def sync_top10(year: int, season: int, progress_cb=None) -> int:
    """
    抓取指定年季的前十大股東。
    progress_cb(done, total) 可選，供 UI 顯示進度。
    回傳寫入筆數。
    """
    init_db()
    roc_year, s = _roc_year_season(year, season)
    year_period = f"{year}Q{season}"
    stocks = get_all_stock_ids()
    total_stocks = len(stocks)
    written = 0

    logger.info("共 %d 支股票，年季：%s", total_stocks, year_period)

    with get_connection() as conn:
        with conn.cursor() as cur:
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
                    except Exception as e:
                        logger.warning("寫入 %s 股東資料失敗: %s", sid, e)

                if (i + 1) % 50 == 0:
                    conn.commit()
                    logger.info("進度 %d/%d，已寫入 %d 筆", i + 1, total_stocks, written)

                if progress_cb:
                    progress_cb(i + 1, total_stocks)

                time.sleep(1.5)

            conn.commit()

    logger.info("前十大股東匯入完成：%d 筆", written)
    return written


if __name__ == "__main__":
    now = datetime.now()
    season = (now.month - 1) // 3 + 1
    sync_top10(now.year, season)
