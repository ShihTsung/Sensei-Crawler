import streamlit as st
import pandas as pd
from database import get_connection
import os
import plotly.express as px

# 1. 頁面配置
st.set_page_config(page_title="Sensei AI 戰略終端", page_icon="🧬", layout="wide")

# 2. 數據加載邏輯
@st.cache_data(ttl=30)
def load_all_data():
    with get_connection() as conn:
        try:
            news_df = pd.read_sql("SELECT * FROM news_summaries ORDER BY created_at DESC", conn)
        except:
            news_df = pd.DataFrame()
        
        try:
            # 撈取資料並計算成交金額 (收盤價 * 成交量)
            stock_query = """
                SELECT 
                    p.stock_id AS "代碼", 
                    p.stock_name AS "公司名稱", 
                    p.close_price AS "收盤價", 
                    p.volume AS "成交量", 
                    (p.close_price * p.volume) AS "成交金額",
                    i.foreign_buy AS "外資買賣超", 
                    i.trust_buy AS "投信買賣超", 
                    i.dealer_buy AS "自營商買賣超"
                FROM twse_prices p
                LEFT JOIN twse_institutional i ON p.stock_id = i.stock_id AND p.date = i.date
                WHERE p.date = '2026-03-30'
                ORDER BY "成交金額" DESC
            """
            stock_df = pd.read_sql(stock_query, conn)
        except:
            stock_df = pd.DataFrame()
            
        return news_df, stock_df

# --- [初始化數據] ---
news_df, stock_df = load_all_data()

st.title("Sensei-Crawler")
tab1, tab2 = st.tabs(["科技情報 Dashboard", "台灣股市基本面"])

# ==========================================
# 🚀 Tab 1: 科技情報內容 (維持原樣)
# ==========================================
with tab1:
    st.write("科技情報內容展示中...")

# ==========================================
# 🏛️ Tab 2: 台灣股市基本面 (優化呈現版)
# ==========================================
with tab2:
    target_date = "2026-03-30"
    st.header(f"🏢 {target_date} 股市行情數據")
    
    if not stock_df.empty:
        # --- 頂部儀表板 ---
        m1, m2 = st.columns([1, 2])
        # 修正：使用 :,d 格式化整數千分位，解決 ValueError
        m1.metric(f"{target_date} 掛牌總數", f"{len(stock_df):,d} 筆")
        
        # --- 排名前 10 筆成交金額 ---
        st.subheader("🔥 成交金額排行前 10 名")
        # 取成交金額最高的前 10 名
        top_10 = stock_df.head(10).copy()
        # 轉換為「億元」顯示，更符合台股閱讀習慣
        top_10["成交金額(億)"] = top_10["成交金額"] / 100_000_000
        
        st.dataframe(
            top_10[["代碼", "公司名稱", "收盤價", "成交金額(億)"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "收盤價": st.column_config.NumberColumn("收盤價", format="%.2f"),
                "成交金額(億)": st.column_config.NumberColumn("成交金額 (億元)", format="%.2f 億")
            }
        )

        st.divider()

        # --- 全量資料搜尋與呈現 ---
        search_q = st.text_input("🔍 搜尋代碼或公司名稱 (如：2330 或 台積電)", "")
        display_df = stock_df.copy()
        if search_q:
            display_df = display_df[display_df['代碼'].str.contains(search_q) | 
                                    display_df['公司名稱'].str.contains(search_q)]

        # --- 完整資料表格式化 ---
        st.dataframe(
            display_df,
            use_container_width=True,
            height=600,
            hide_index=True,
            column_config={
                "代碼": st.column_config.TextColumn("代碼"),
                "公司名稱": st.column_config.TextColumn("公司名稱"),
                "收盤價": st.column_config.NumberColumn("收盤價", format="%.2f"),
                "成交量": st.column_config.NumberColumn("成交量 (股)", format="%,d"),
                "成交金額": st.column_config.NumberColumn("成交金額 (元)", format="%,d"),
                "外資買賣超": st.column_config.NumberColumn("外資買賣超", format="%,d"),
                "投信買賣超": st.column_config.NumberColumn("投信買賣超", format="%,d"),
                "自營商買賣超": st.column_config.NumberColumn("自營商買賣超", format="%,d"),
            }
        )
    else:
        st.warning(f"資料庫中尚無 {target_date} 的行情數據。")