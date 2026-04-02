import streamlit as st
import pandas as pd
from database import get_connection
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(layout="wide", page_title="台股戰略終端")
pd.set_option("styler.render.max_elements", 1000000)

# ── 持股分級說明 ──────────────────────────────────────────────
LEVEL_LABELS = {
    1:  "1~999 股", 2:  "1,000~5,000 股", 3:  "5,001~10,000 股",
    4:  "10,001~15,000 股", 5:  "15,001~20,000 股", 6:  "20,001~30,000 股",
    7:  "30,001~40,000 股", 8:  "40,001~50,000 股", 9:  "50,001~100,000 股",
    10: "100,001~200,000 股", 11: "200,001~400,000 股", 12: "400,001~600,000 股",
    13: "600,001~800,000 股", 14: "800,001~1,000,000 股", 15: "1,000,001 股以上"
}

# ── 資料載入函數 ──────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_all_data(date_str):
    with get_connection() as conn:
        # 【關鍵修復】使用 COALESCE 將所有的 NULL 轉換為 0，避免 format 報錯
        query = f"""
            SELECT 
                p.stock_id AS 代碼, 
                p.stock_name AS 名稱, 
                COALESCE(c.category_name, '未分類') AS 新產業類別,
                COALESCE(p.close_price, 0) AS 收盤, 
                COALESCE(p.trade_volume, 0) AS 成交量,
                COALESCE(p.trade_value, 0) AS 成交金額,
                COALESCE(i.foreign_net_buy, 0) AS 外資買賣超,
                COALESCE(i.trust_net_buy, 0) AS 投信買賣超
            FROM twse_prices p
            LEFT JOIN twse_institutional_investors i ON p.stock_id = i.stock_id AND p.date = i.date
            LEFT JOIN stock_category c ON p.stock_id = c.stock_id
            WHERE p.date = '{date_str}'
            AND p.stock_id ~ '^[0-9]+$' AND length(p.stock_id) = 4
            ORDER BY p.trade_value DESC
        """
        return pd.read_sql(query, conn)

def get_available_dates():
    with get_connection() as conn:
        query = "SELECT DISTINCT date FROM twse_prices ORDER BY date DESC"
        df = pd.read_sql(query, conn)
        return df['date'].tolist()

# ── 集保分析對話框 ──────────────────────────────────────────────
@st.dialog("個股深度分析")
def show_concentration_dialog(sid, name):
    st.subheader(f"{sid} {name}")
    
    tab1, tab2, tab3 = st.tabs(["最新持股分級", "大戶比例趨勢", "📊 價量趨勢與分析"])
    
    with tab1:
        with get_connection() as conn:
            query = f"SELECT * FROM twse_weekly_concentration WHERE stock_id = '{sid}' ORDER BY date DESC"
            df = pd.read_sql(query, conn)
        if df.empty:
            st.warning("資料庫尚無此股票的集保週資料。")
        else:
            latest_date = df['date'].iloc[0]
            st.write(f"**資料日期：** {latest_date}")
            df_latest = df[df['date'] == latest_date].copy()
            df_latest['分級說明'] = df_latest['level'].map(LEVEL_LABELS)
            st.dataframe(
                df_latest[['level', '分級說明', 'holders', 'shares', 'rate']],
                column_config={
                    "level": "級距", "holders": "人數", 
                    "shares": "股數", "rate": st.column_config.ProgressColumn("占總股數比例(%)", min_value=0, max_value=100, format="%.2f%%")
                },
                hide_index=True,
                use_container_width=True
            )

    with tab2:
        if not df.empty:
            df_large = df[df['level'] == 15].copy()
            if not df_large.empty:
                df_large = df_large.sort_values("date")
                st.write("**大戶（100萬股以上）持股比例走勢**")
                st.line_chart(data=df_large, x="date", y="rate", use_container_width=True)
            else:
                st.info("尚無大戶持股資料。")

    with tab3:
        with get_connection() as conn:
            price_query = f"""
                SELECT date, close_price, trade_volume 
                FROM twse_prices 
                WHERE stock_id = '{sid}' 
                ORDER BY date ASC
            """
            price_df = pd.read_sql(price_query, conn)
            
        if not price_df.empty:
            # 將 date 轉為 datetime，圖表才會連續且美觀
            price_df['date'] = pd.to_datetime(price_df['date'], format='%Y%m%d')
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Bar(x=price_df['date'], y=price_df['trade_volume'], name="成交量", marker_color='rgba(135, 206, 235, 0.5)'),
                secondary_y=True,
            )
            fig.add_trace(
                go.Scatter(x=price_df['date'], y=price_df['close_price'], name="收盤價", mode='lines', line=dict(color='firebrick', width=2)),
                secondary_y=False,
            )
            
            fig.update_layout(
                title_text="歷史價格與成交量",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            fig.update_yaxes(title_text="<b>價格</b>", secondary_y=False)
            fig.update_yaxes(title_text="<b>成交量</b>", secondary_y=True, showgrid=False)
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("資料庫中尚無此股票的歷史價量資料。")

# ── 側邊欄控制區 ──────────────────────────────────────────────
st.sidebar.header("📅 選擇交易日")
available_dates = get_available_dates()

if not available_dates:
    st.error("資料庫中沒有價格資料，請先執行爬蟲。")
    st.stop()

display_dates = [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in available_dates]
selected_display_date = st.sidebar.selectbox("交易日", display_dates)
target_date = available_dates[display_dates.index(selected_display_date)]

st.sidebar.markdown("---")
st.sidebar.subheader("🔎 個股集保分析")
lookup_id = st.sidebar.text_input("輸入股票代碼", placeholder="例如：2330")

# ── 畫面主體：載入資料 ──────────────────────────────────────────
stock_df = load_all_data(target_date)

if lookup_id:
    sid = lookup_id.strip()
    match = stock_df[stock_df["代碼"] == sid] if not stock_df.empty else pd.DataFrame()
    name = match.iloc[0]["名稱"] if not match.empty else ""
    label = f"📊 開啟 {sid} {name} 集保分析" if name else f"📊 開啟 {sid} 集保分析"
    if st.sidebar.button(label, use_container_width=True):
        show_concentration_dialog(sid, name)

st.title(f"台股 — {selected_display_date} 數據報表")

if stock_df.empty:
    st.warning(f"⚠️ {selected_display_date} 尚無資料。")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("顯示家數（已過濾權證）", f"{len(stock_df)} 家")
col2.metric("總成交金額", f"{stock_df['成交金額'].sum() / 1e8:.2f} 億")
col3.metric("查詢日期", selected_display_date)

search_query = st.text_input("🔍 搜尋股票代碼或名稱", "")
if search_query:
    stock_df = stock_df[
        stock_df["代碼"].str.contains(search_query, na=False) |
        stock_df["名稱"].str.contains(search_query, na=False)
    ]

# 這裡也同步移除了可能會報錯的欄位格式化，確保與上方 SQL 撈取的欄位一致
st.dataframe(
    stock_df.style.format({
        "收盤": "{:.2f}", 
        "成交量": "{:,.0f}", 
        "成交金額": "{:,.0f}",
        "外資買賣超": "{:,.0f}", 
        "投信買賣超": "{:,.0f}"
    }),
    use_container_width=True,
    hide_index=True,
    height=600
)