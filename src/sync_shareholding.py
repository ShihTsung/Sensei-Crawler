import logging
import re
import time
from io import StringIO

import pandas as pd
import requests
from psycopg2.extras import execute_values

from database import get_connection

logger = logging.getLogger(__name__)


def clean(val):
    if val is None or pd.isna(val):
        return None
    val = re.sub(r'<[^>]+>', '', str(val))
    val = val.replace(',', '').strip()
    return None if val in ['--', '---', ''] else val


def sync_insider_holding(year: int, month: int):
    roc_year = year - 1911 if year > 1911 else year
    date_str = f"{year}{str(month).zfill(2)}"
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://mops.twse.com.tw/mops/web/t08get02",
    }
    url = "https://mops.twse.com.tw/mops/web/ajax_t08get02"
    payload = {
        'encodeutf8': '1', 'step': '1', 'firstin': '1',
        'off': '1', 'TYPEK': 'all',
        'year': str(roc_year), 'month': str(month).zfill(2),
    }

    logger.info("正在請求 %d年%d月 董監持股...", year, month)
    try:
        session.get("https://mops.twse.com.tw/mops/web/t08get02", headers=headers, timeout=20)
        time.sleep(2)
        response = session.post(url, data=payload, headers=headers, timeout=30)
        response.encoding = 'utf-8'

        if "查詢無資料" in response.text:
            logger.warning("查無 %s 資料。", date_str)
            return

        dfs = pd.read_html(StringIO(response.text))
        df = next((t for t in dfs if '公司代號' in t.columns and '姓名' in t.columns), None)
        if df is None:
            logger.error("找不到表格，請確認 MOPS 回應格式。")
            return

        rows = []
        for _, row in df.iterrows():
            sid = str(row['公司代號']).split('.')[0].strip()
            if not sid.isdigit() or not (4 <= len(sid) <= 6):
                continue
            rows.append((sid, date_str, str(row['職稱']), str(row['姓名']),
                         clean(row['目前持股']), clean(row['質押股數']), clean(row['質押比例'])))

        if not rows:
            logger.warning("%s 無有效資料", date_str)
            return

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 0")
                execute_values(cur, """
                    INSERT INTO twse_insider_holding
                        (stock_id, date, title, name, held_shares, pledged_shares, pledge_rate)
                    VALUES %s
                    ON CONFLICT (stock_id, date, name) DO UPDATE SET
                        held_shares    = EXCLUDED.held_shares,
                        pledged_shares = EXCLUDED.pledged_shares
                """, rows)
                conn.commit()
        logger.info("%s 導入成功（%d 筆）", date_str, len(rows))
    except Exception as e:
        logger.error("執行錯誤: %s", e)


if __name__ == "__main__":
    sync_insider_holding(2025, 12)
