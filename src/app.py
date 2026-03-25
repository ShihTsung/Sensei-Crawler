import streamlit as st
import pandas as pd
import plotly.express as px
from database import get_connection

# 1. 網頁基本設定
st.set_page_config(page_title="Sensei AI 情報中心", page_icon="🚀", layout="wide")

# 2. 從資料庫讀取資料的函式
def load_data():
    try:
        with get_connection() as conn:
            query = "SELECT title, company, sentiment, created_at, summary, url FROM news_summaries ORDER BY created_at DESC"
            # 使用 pandas 讀取 SQL 數據
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"資料讀取失敗: {e}")
        return pd.DataFrame()

# --- 網頁前端介面 ---
st.title("🚀 Sensei AI 科技情報 Dashboard")
st.markdown("這是自動化抓取並經由 Llama 3 總結的產業動態。")

df = load_data()

if df.empty:
    st.warning("目前資料庫中沒有資料，請先執行 python src/summarizer.py 抓取新聞。")
else:
    # --- 頂部指標 (Metrics) ---
    col1, col2, col3 = st.columns(3)
    col1.metric("今日情報總數", len(df))
    col2.metric("最新來源", df['company'].iloc[0])
    col3.metric("資料更新日期", df['created_at'].iloc[0].strftime('%Y-%m-%d'))

    st.divider()

    # --- 視覺化圖表區 ---
    st.subheader("📊 產業情緒與重點摘要")
    c1, c2 = st.columns([2, 3])
    
    with c1:
        # 情緒分佈圓餅圖
        sentiment_counts = df['sentiment'].value_counts().reset_index()
        sentiment_counts.columns = ['sentiment', 'count']
        fig = px.pie(sentiment_counts, values='count', names='sentiment', 
                     title="AI 判定情緒分佈",
                     color='sentiment', 
                     color_discrete_map={'正面':'#2ecc71', '中立':'#95a5a6', '負面':'#e74c3c'})
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        # 顯示最近的三則新聞卡片
        st.write("🔍 **最新動態快速看**")
        for i in range(min(3, len(df))):
            st.info(f"**{df['title'].iloc[i]}** ({df['company'].iloc[i]})")

    st.divider()

    # --- 詳細清單區 (用摺疊選單呈現) ---
    st.subheader("📰 所有情報詳細清單")
    for index, row in df.iterrows():
        with st.expander(f"{row['created_at'].strftime('%H:%M')} | {row['title']} ({row['company']})"):
            st.write(f"**AI 判定**: {row['sentiment']}")
            st.write("**核心重點摘要**:")
            for point in row['summary']:
                st.write(f"🔹 {point}")
            st.link_button("🔗 閱讀原文", row['url'])