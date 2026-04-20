"""
sync_company_info.py
從 TWSE / TPEx OpenAPI 一次拉取全市場公司基本資料。
資料幾乎不變，建議每年更新一次或手動觸發。
"""

import logging
import time

import requests

from database import get_connection
from http_utils import with_retry

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0"}

SOURCES = [
    {"market": "上市", "url": "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"},
    {"market": "上櫃", "url": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"},
]


def _get(item: dict, *keys, default=""):
    for k in keys:
        v = item.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return default


def _parse_capital(val) -> int | None:
    try:
        return int(str(val).replace(",", "").replace(" ", ""))
    except Exception:
        return None


@with_retry()
def _fetch_source(url: str) -> list:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def sync_company_info() -> int:
    """抓取上市、上櫃公司基本資料，寫入 company_info 表，回傳寫入筆數。"""
    total = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS company_info (
                    stock_id        VARCHAR(10) PRIMARY KEY,
                    tax_id          VARCHAR(20),
                    company_name    VARCHAR(100),
                    market_type     VARCHAR(10),
                    address         VARCHAR(300),
                    phone           VARCHAR(50),
                    fax             VARCHAR(50),
                    chairman        VARCHAR(50),
                    ceo             VARCHAR(50),
                    spokesperson    VARCHAR(50),
                    capital         BIGINT,
                    listed_date     VARCHAR(20),
                    website         VARCHAR(200),
                    updated_at      TIMESTAMP DEFAULT NOW()
                );
            """)

            for src in SOURCES:
                logger.info("抓取 %s 基本資料...", src['market'])
                try:
                    data = _fetch_source(src["url"])
                except Exception as e:
                    logger.error("%s 失敗: %s", src['market'], e)
                    continue

                count = 0
                for item in data:
                    sid = _get(item, "公司代號", "SecuritiesCompanyCode")
                    if not sid or not sid.isdigit() or not (4 <= len(sid) <= 6):
                        continue

                    cur.execute("""
                        INSERT INTO company_info
                            (stock_id, tax_id, company_name, market_type,
                             address, phone, fax, chairman, ceo, spokesperson,
                             capital, listed_date, website, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (stock_id) DO UPDATE SET
                            tax_id        = EXCLUDED.tax_id,
                            company_name  = EXCLUDED.company_name,
                            market_type   = EXCLUDED.market_type,
                            address       = EXCLUDED.address,
                            phone         = EXCLUDED.phone,
                            fax           = EXCLUDED.fax,
                            chairman      = EXCLUDED.chairman,
                            ceo           = EXCLUDED.ceo,
                            spokesperson  = EXCLUDED.spokesperson,
                            capital       = EXCLUDED.capital,
                            listed_date   = EXCLUDED.listed_date,
                            website       = EXCLUDED.website,
                            updated_at    = NOW()
                    """, (
                        sid,
                        _get(item, "統一編號"),
                        _get(item, "公司名稱", "CompanyName"),
                        src["market"],
                        _get(item, "住址", "地址", "Address"),
                        _get(item, "電話", "Phone"),
                        _get(item, "傳真", "Fax"),
                        _get(item, "董事長", "Chairman"),
                        _get(item, "總經理", "GeneralManager"),
                        _get(item, "發言人", "Spokesperson"),
                        _parse_capital(_get(item, "實收資本額", "Capital")),
                        _get(item, "上市日期", "ListingDate"),
                        _get(item, "網址", "Website"),
                    ))
                    count += 1

                conn.commit()
                total += count
                logger.info("%s：%d 家", src['market'], count)
                time.sleep(2)

    logger.info("公司基本資料匯入完成：%d 筆", total)
    return total


if __name__ == "__main__":
    sync_company_info()
