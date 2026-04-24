import pandas as pd
import streamlit as st

from database import get_connection


def get_all_available_dates() -> list[str]:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT date FROM twse_prices ORDER BY date DESC")
                return [r[0] for r in cur.fetchall()]
    except Exception as e:
        st.error(f"無法讀取日期清單: {e}")
        return []


@st.cache_data(ttl=300)
def load_all_data(date_str: str) -> pd.DataFrame:
    query = """
        SELECT
            p.stock_id         AS "代碼",
            p.stock_name       AS "名稱",
            sc.category_name   AS "產業",
            p.open_price       AS "開盤",
            p.high_price       AS "最高",
            p.low_price        AS "最低",
            p.close_price      AS "收盤",
            p.price_change_dir AS "漲跌方向",
            p.price_change     AS "漲跌",
            p.pe_ratio         AS "本益比",
            p.trade_volume     AS "成交量",
            p.trade_value      AS "成交金額",
            i.foreign_buy      AS "外資買進",
            i.foreign_sell     AS "外資賣出",
            i.foreign_net      AS "外資淨額",
            i.trust_net        AS "投信淨額",
            i.dealer_net       AS "自營商淨額"
        FROM twse_prices p
        LEFT JOIN twse_institutional i
               ON p.stock_id = i.stock_id AND p.date = i.date
        LEFT JOIN stock_category sc
               ON p.stock_id = sc.stock_id
        WHERE p.date = %s
          AND LENGTH(p.stock_id) <= 5
        ORDER BY p.trade_value DESC NULLS LAST
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date_str,))
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)


@st.cache_data(ttl=300)
def load_concentration(stock_id: str) -> pd.DataFrame:
    query = """
        SELECT date, level, holders, shares, rate
        FROM twse_weekly_concentration
        WHERE stock_id = %s
          AND level NOT IN (16, 17)
        ORDER BY date DESC, level ASC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (stock_id,))
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
