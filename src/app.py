import streamlit as st
import pandas as pd
from database import get_connection
from datetime import datetime
import pytz

# 設定網頁寬度為全螢幕，並解決渲染上限問題
st.set_page_config(layout="wide", page_title="台股戰略終端")
pd.set_option("styler.render.max_elements", 1000000)

# 1. 取得資料庫中「所有」有資料的日期清單
def get_all_available_dates():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 抓取不重複的日期，並由新到舊排序
                cur.execute("SELECT DISTINCT date FROM twse_prices ORDER BY date DESC")
                results = cur.fetchall()
                return [r[0] for r in results]
    except Exception as e:
        st.error(f"無法讀取日期清單: {e}")
        return []

# 2. 數據加載邏輯
@st.cache_data(ttl=60)
def load_all_data(date_str):
    with get_connection() as conn:
        stock_query = f"""
            SELECT 
                p.stock_id AS "代碼", 
                p.stock_name AS "名稱", 
                p.open_price AS "開盤",
                p.high_price AS "最高",
                p.low_price AS "最低",
                p.close_price AS "收盤",
                p.price_change AS "漲跌",
                p.pe_ratio AS "本益比",
                p.trade_volume AS "成交量", 
                p.trade_value AS "成交金額",
                i.foreign_buy AS "外資買進", 
                i.foreign_sell AS "外資賣出", 
                i.foreign_net AS "外資淨額",
                i.trust_net AS "投信淨額", 
                i.dealer_net AS "自營商淨額"
            FROM twse_prices p
            LEFT JOIN twse_institutional i ON p.stock_id = i.stock_id AND p.date = i.date
            WHERE p.date = '{date_str}'
            ORDER BY "成交金額" DESC
        """
        df = pd.read_sql(stock_query, conn)
        
        # 過濾掉權證 (代碼長度 > 5)
        if not df.empty:
            df = df[df['代碼'].str.len() <= 5]
            
        return df

# --- 執行主邏輯 ---

# A. 取得所有日期選項
all_dates = get_all_available_dates()

if all_dates:
    # B. 建立側邊欄日期選擇器
    st.sidebar.header("🗓️ 歷史存檔切換")
    
    # 格式化日期顯示 (YYYYMMDD -> YYYY-MM-DD)
    date_options = {datetime.strptime(d, '%Y%m%d').strftime('%Y-%m-%d'): d for d in all_dates}
    selected_display_date = st.sidebar.selectbox("請選擇交易日期", options=list(date_options.keys()))
    
    # 取得實際要查詢的 YYYYMMDD 字串
    target_date = date_options[selected_display_date]
    
    # C. 載入選定日期的數據
    stock_df = load_all_data(target_date)
    
    st.title(f"台股 - {selected_display_date} 數據報表")
    
    if not stock_df.empty:
        # 數據概覽儀表板
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("顯示家數 (已過濾權證)", f"{len(stock_df)} 家")
        with col2:
            total_amt = stock_df["成交金額"].sum() / 1e8
            st.metric("總成交金額", f"{total_amt:.2f} 億")
        with col3:
            st.metric("當前查詢日期", selected_display_date)

        # 搜尋功能
        search_query = st.text_input("🔍 搜尋股票代碼或名稱", "")
        if search_query:
            stock_df = stock_df[
                stock_df['代碼'].str.contains(search_query) | 
                stock_df['名稱'].str.contains(search_query)
            ]
        
        # 呈現表格並美化格式
        st.dataframe(
            stock_df.style.format({
                "收盤": "{:.2f}", "開盤": "{:.2f}", "最高": "{:.2f}", "最低": "{:.2f}",
                "漲跌": "{:+.2f}", "本益比": "{:.2f}", "成交量": "{:,}", "成交金額": "{:,.0f}"
            }, na_rep="-"), 
            use_container_width=True, 
            height=700
        )
    else:
        st.warning(f"⚠️ {selected_display_date} 尚無符合條件的數據內容。")
else:
    st.error("⚠️ 資料庫目前是空的，請執行同步腳本抓取資料。")