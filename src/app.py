import streamlit as st
import pandas as pd
from database import get_connection
import os
import plotly.express as px
from datetime import date

# 1. 頁面配置：全螢幕佈局，隱藏側邊欄
st.set_page_config(page_title="Sensei AI 戰略終端", page_icon="🧬", layout="wide", initial_sidebar_state="collapsed")

# 2. 數據加載邏輯
@st.cache_data(ttl=30)
def load_all_data():
    with get_connection() as conn:
        # A. 企業數據
        try:
            df = pd.read_sql("SELECT * FROM companies", conn)
        except:
            df = pd.DataFrame()
        
        # B. 科技情報數據
        try:
            # 確保對接 news_summaries 表
            news_df = pd.read_sql("SELECT * FROM news_summaries ORDER BY created_at DESC", conn)
        except:
            news_df = pd.DataFrame()
            
        return df, news_df

# --- [初始化數據] ---
df, news_df = load_all_data()

st.title("Sensei-Crawler")

# 使用 Tabs 切換
tab_news, tab_stock = st.tabs(["科技情報 Dashboard", "台灣股市基本面"])

# ==========================================
# 🚀 Tab 1: 科技情報 (包含優化後的啟動邏輯)
# ==========================================
with tab_news:
    c1, c2, c3, c4 = st.columns([1, 1, 1, 1.5])
    c1.metric("今日情報總數", len(news_df))
    latest_src = news_df['company'].iloc[0] if not news_df.empty else "N/A"
    c2.metric("最新來源", latest_src)
    c3.metric("資料更新日期", str(date.today()))
    
    # 🎯 核心啟動鈕：整合狀態顯示與非同步提示
    if c4.button("執行 AI 深度採集與分析", use_container_width=True, type="primary"):
        with st.status("🤖 Sensei 正在全力運算中...", expanded=True) as status:
            st.write("📡 正在掃描 RSS 來源並抓取內文...")
            st.info("💡 提示：本地 CPU 負載較高，請觀察終端機 (PowerShell) 的實時進度。")
            
            # 執行採集與 AI 分析
            os.system("python src/summarizer.py")
            
            # 清除快取並刷新頁面
            st.cache_data.clear()
            status.update(label="✅ 分析任務完成！", state="complete", expanded=False)
        st.rerun()

    st.divider()
    col_a, col_b = st.columns([1, 2])
    
    with col_a:
        st.subheader("📊 產業情緒分布")
        if not news_df.empty:
            fig = px.pie(news_df, names='sentiment', hole=0.3, 
                         color_discrete_map={'正面':'#2ecc71', '負面':'#e74c3c', '中立':'#95a5a6'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("暫無情報數據，請按下上方啟動鈕。")
            
    with col_b:
        st.subheader("🔍 重點速覽")
        if not news_df.empty:
            for i, row in news_df.head(4).iterrows():
                st.info(f"**{row['title']}** ({row['company']})")

    st.subheader("📰 所有情報詳細清單")
    for i, row in news_df.iterrows():
        time_str = row['created_at'].strftime('%H:%M') if hasattr(row['created_at'], 'strftime') else "00:00"
        with st.expander(f"{time_str} | {row['title']} ({row['company']})"):
            st.write(f"**AI 判定：** {row['sentiment']}")
            # 處理 TEXT[] 清單格式
            points = row['summary']
            if isinstance(points, list):
                for p in points: st.write(f"- {p}")
            else:
                st.write(points)
            st.write(f"[🔗 閱讀原文]({row['url']})")

# ==========================================
# 🏛️ Tab 2: 企業檢索百科 (移除錯誤重複按鈕)
# ==========================================
with tab_stock:
    with st.container(border=True):
        st.markdown("### 🛰️ 戰略控制塔")
        cm1, cm2, cm3 = st.columns([1.5, 1.5, 1])
        with cm1:
            mkt_list = sorted(list(df['market_type'].unique())) if not df.empty else []
            sel_mkt = st.multiselect("市場過濾", mkt_list, default=["上市"] if "上市" in mkt_list else None)
        with cm2:
            ai_threshold = st.select_slider("AI 潛力門檻", options=[0.0, 0.5, 0.7, 0.8, 0.9], value=0.0)
        with cm3:
            st.write("")
            if st.button("📈 同步行情數據", use_container_width=True):
                os.system("python src/price_sync.py")
                st.cache_data.clear()
                st.rerun()
with tab2:
    st.header("🏢 企業戰略百科")
    company_name = st.text_input("輸入企業名稱 (例如：NVIDIA, 台積電, Fubon momo)", placeholder="請輸入欲檢索的企業...")
    
    if st.button("🔍 開始深度檢索"):
        if company_name:
            with st.spinner(f"正在調用 Gemini 2.5 進行 {company_name} 的戰略分析..."):
                # 這裡呼叫我們待會要寫的百科分析函數
                from src.encyclopedia import get_company_profile
                profile = get_company_profile(company_name)
                
                if profile:
                    st.success(f"已完成 {company_name} 的戰略建模")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("📌 核心業務與定位")
                        st.write(profile.get("core_business"))
                    with col2:
                        st.subheader("🚀 技術架構建議")
                        st.info(profile.get("tech_stack_advice"))
                        
                    st.divider()
                    st.subheader("📊 SWOT 戰略分析")
                    st.json(profile.get("swot"))
        else:
            st.warning("請先輸入公司名稱。")
    st.divider()
    q = st.text_input("🔍 全文搜尋企業...", placeholder="2330 或 台積電")
    
    # 篩選邏輯
    f_df = df.copy()
    if sel_mkt: f_df = f_df[f_df['market_type'].isin(sel_mkt)]
    f_df = f_df[f_df['ai_relevance'] >= ai_threshold]
    if q: 
        f_df = f_df[f_df['stock_id'].astype(str).str.contains(q) | f_df['company_name'].str.contains(q)]

    st.dataframe(f_df, use_container_width=True, hide_index=True)