import streamlit as st
import pandas as pd
from database import get_connection
from datetime import datetime

st.set_page_config(layout="wide", page_title="台股戰略終端")
pd.set_option("styler.render.max_elements", 1000000)

# ── 持股分級說明 ──────────────────────────────────────────────
LEVEL_LABELS = {
    1:  "1~999 股",
    2:  "1,000~5,000 股",
    3:  "5,001~10,000 股",
    4:  "10,001~15,000 股",
    5:  "15,001~20,000 股",
    6:  "20,001~30,000 股",
    7:  "30,001~40,000 股",
    8:  "40,001~50,000 股",
    9:  "50,001~100,000 股",
    10: "100,001~200,000 股",
    11: "200,001~400,000 股",
    12: "400,001~600,000 股",
    13: "600,001~800,000 股",
    14: "800,001~1,000,000 股",
    15: "超過 1,000,000 股",
    16: "合計",
    17: "自然人合計",
}

# ── 資料讀取函式 ──────────────────────────────────────────────

def get_all_available_dates():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT date FROM twse_prices ORDER BY date DESC")
                return [r[0] for r in cur.fetchall()]
    except Exception as e:
        st.error(f"無法讀取日期清單: {e}")
        return []

@st.cache_data(ttl=60)
def load_all_data(date_str):
    with get_connection() as conn:
        query = f"""
            SELECT
                p.stock_id  AS "代碼",
                p.stock_name AS "名稱",
                p.open_price  AS "開盤",
                p.high_price  AS "最高",
                p.low_price   AS "最低",
                p.close_price AS "收盤",
                p.price_change AS "漲跌",
                p.pe_ratio    AS "本益比",
                p.trade_volume AS "成交量",
                p.trade_value  AS "成交金額",
                i.foreign_buy  AS "外資買進",
                i.foreign_sell AS "外資賣出",
                i.foreign_net  AS "外資淨額",
                i.trust_net    AS "投信淨額",
                i.dealer_net   AS "自營商淨額"
            FROM twse_prices p
            LEFT JOIN twse_institutional i
                   ON p.stock_id = i.stock_id AND p.date = i.date
            WHERE p.date = '{date_str}'
            ORDER BY "成交金額" DESC NULLS LAST
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            df = df[df["代碼"].str.len() <= 5]
        return df

@st.cache_data(ttl=300)
def load_concentration(stock_id: str):
    """
    回傳該股所有週的持股分級資料（排除 level 16/17 合計列）。
    """
    with get_connection() as conn:
        query = f"""
            SELECT date, level, holders, shares, rate
            FROM twse_weekly_concentration
            WHERE stock_id = '{stock_id}'
              AND level NOT IN (16, 17)
            ORDER BY date DESC, level ASC
        """
        return pd.read_sql(query, conn)

# ── 集保詳細 Dialog ───────────────────────────────────────────

@st.dialog("📊 集保持股分析", width="large")
def show_concentration_dialog(stock_id: str, stock_name: str):
    st.markdown(f"### {stock_id}　{stock_name}")

    df = load_concentration(stock_id)

    if df.empty:
        st.warning("資料庫尚無此股票的集保週資料。")
        return

    dates = sorted(df["date"].unique(), reverse=True)

    # ── Tab 1：最新一週分級表 ──────────────────────────────
    tab1, tab2 = st.tabs(["📋 最新持股分級", "📈 大戶比例趨勢"])

    with tab1:
        latest = dates[0]
        st.caption(f"資料日期：{latest}")

        latest_df = (
            df[df["date"] == latest]
            .copy()
            .assign(持股區間=lambda x: x["level"].map(LEVEL_LABELS))
            [["持股區間", "holders", "shares", "rate"]]
            .rename(columns={"holders": "人數", "shares": "股數", "rate": "佔比 %"})
            .reset_index(drop=True)
        )

        st.dataframe(
            latest_df.style.format({
                "人數": "{:,}",
                "股數": "{:,}",
                "佔比 %": "{:.2f}",
            }, na_rep="-"),
            use_container_width=True,
            height=420,
        )

    # ── Tab 2：大戶（level 15，超過百萬股）比例趨勢 ──────────
    with tab2:
        # 大戶 = level 15（超過 1,000,000 股）
        whale_df = (
            df[df["level"] == 15]
            .sort_values("date")
            .rename(columns={"rate": "大戶佔比 %", "holders": "大戶人數"})
        )

        if whale_df.empty:
            st.info("尚無足夠週資料可繪製趨勢圖。")
        else:
            st.caption("大戶定義：單一帳戶持股超過 1,000,000 股（level 15）")

            col_a, col_b = st.columns(2)
            with col_a:
                st.metric(
                    "最新大戶佔比",
                    f"{whale_df['大戶佔比 %'].iloc[-1]:.2f} %",
                    delta=f"{whale_df['大戶佔比 %'].iloc[-1] - whale_df['大戶佔比 %'].iloc[-2]:.2f} %" 
                          if len(whale_df) >= 2 else None,
                )
            with col_b:
                st.metric(
                    "最新大戶人數",
                    f"{int(whale_df['大戶人數'].iloc[-1]):,} 人",
                    delta=f"{int(whale_df['大戶人數'].iloc[-1]) - int(whale_df['大戶人數'].iloc[-2]):+,} 人"
                          if len(whale_df) >= 2 else None,
                )

            st.line_chart(
                whale_df.set_index("date")[["大戶佔比 %"]],
                use_container_width=True,
                height=280,
            )

            # 附上原始數據
            with st.expander("查看所有週資料"):
                st.dataframe(
                    whale_df[["date", "大戶人數", "大戶佔比 %"]]
                    .sort_values("date", ascending=False)
                    .reset_index(drop=True)
                    .style.format({"大戶人數": "{:,}", "大戶佔比 %": "{:.2f}"}),
                    use_container_width=True,
                )

# ── 主畫面 ────────────────────────────────────────────────────

all_dates = get_all_available_dates()

if not all_dates:
    st.error("⚠️ 資料庫目前是空的，請執行同步腳本抓取資料。")
    st.stop()

# 側邊欄
st.sidebar.header("🗓️ 歷史存檔切換")
date_options = {
    datetime.strptime(d, "%Y%m%d").strftime("%Y-%m-%d"): d
    for d in all_dates
}
selected_display_date = st.sidebar.selectbox("請選擇交易日期", list(date_options.keys()))
target_date = date_options[selected_display_date]

# 載入資料
stock_df = load_all_data(target_date)

st.title(f"台股 — {selected_display_date} 數據報表")

if stock_df.empty:
    st.warning(f"⚠️ {selected_display_date} 尚無資料。")
    st.stop()

# 儀表板
col1, col2, col3 = st.columns(3)
col1.metric("顯示家數（已過濾權證）", f"{len(stock_df)} 家")
col2.metric("總成交金額", f"{stock_df['成交金額'].sum() / 1e8:.2f} 億")
col3.metric("查詢日期", selected_display_date)

# 搜尋
search_query = st.text_input("🔍 搜尋股票代碼或名稱", "")
if search_query:
    stock_df = stock_df[
        stock_df["代碼"].str.contains(search_query, na=False) |
        stock_df["名稱"].str.contains(search_query, na=False)
    ]

st.caption("💡 點擊下方按鈕可查看該股集保持股分析")

# 每列顯示股票資料 + 「集保分析」按鈕
# 用 st.dataframe 顯示主表，另外用按鈕觸發 dialog
st.dataframe(
    stock_df.style.format({
        "收盤": "{:.2f}", "開盤": "{:.2f}", "最高": "{:.2f}", "最低": "{:.2f}",
        "漲跌": "{:+.2f}", "本益比": "{:.2f}",
        "成交量": "{:,}", "成交金額": "{:,.0f}",
    }, na_rep="-"),
    use_container_width=True,
    height=500,
)

st.divider()
st.subheader("🔎 個股集保分析")

# 輸入代碼開啟集保 dialog
lookup_id = st.text_input("輸入股票代碼查集保資料", placeholder="例如：2330")
if lookup_id:
    match = stock_df[stock_df["代碼"] == lookup_id.strip()]
    if not match.empty:
        name = match.iloc[0]["名稱"]
        if st.button(f"📊 開啟 {lookup_id} {name} 集保分析"):
            show_concentration_dialog(lookup_id.strip(), name)
    else:
        # 不在今日行情也能查（直接用代碼）
        if st.button(f"📊 開啟 {lookup_id} 集保分析"):
            show_concentration_dialog(lookup_id.strip(), "")
