import streamlit as st
import pandas as pd
from database import get_connection

st.set_page_config(page_title="Sensei Market Viewer", layout="wide")

st.title("📊 證交所歷史資料觀測站")
st.subheader("日期：2026-03-30")

@st.cache_data
def load_data():
    with get_connection() as conn:
        # 使用 JOIN 把行情與三大法人籌碼合併在一起呈現
        query = """
        SELECT 
            p.stock_id AS "代號",
            p.stock_name AS "名稱",
            p.close_price AS "收盤價",
            p.volume AS "成交量",
            i.foreign_buy AS "外資買賣超",
            i.trust_buy AS "投信買賣超",
            i.dealer_buy AS "自營商買賣超"
        FROM twse_prices p
        LEFT JOIN twse_institutional i ON p.stock_id = i.stock_id AND p.date = i.date
        WHERE p.date = '2026-03-30'
        ORDER BY p.stock_id ASC
        """
        df = pd.read_sql(query, conn)
    return df

try:
    df = load_data()

    # 頂部儀表板
    col1, col2, col3 = st.columns(3)
    col1.metric("總標的數", f"{len(df)} 筆")
    col2.metric("總成交量", f"{df['成交量'].sum():,.0f}")
    
    # 搜尋功能
    search_query = st.text_input("🔍 輸入代號或名稱搜尋", "")
    if search_query:
        df = df[df['代號'].str.contains(search_query) | df['名稱'].str.contains(search_query)]

    # 呈現資料表格
    st.dataframe(df, use_container_width=True, height=600)

except Exception as e:
    st.error(f"資料撈取失敗: {e}")
    st.info("請確認資料庫中是否有 2026-03-30 的資料。")