import streamlit as st
import pandas as pd
from datetime import datetime

from data.queries import get_all_available_dates, load_all_data
from ui.admin import render_admin_panel
from ui.constants import FMT_MAP, MOBILE_COLS
from ui.dialogs import show_concentration_dialog

st.set_page_config(layout="wide", page_title="Sensei")
pd.set_option("styler.render.max_elements", 1000000)

st.markdown("""
<style>
/* 主標題縮小 */
h1 { font-size: 0.875rem !important; font-weight: normal !important; color: #888 !important; }

/* 指標卡片縮小：value 與 label 統一大小 */
[data-testid="metric-container"] { padding: 4px 8px !important; }
[data-testid="metric-container"] [data-testid="metric-value"],
[data-testid="metric-container"] [data-testid="stMetricValue"],
[data-testid="metric-container"] [data-testid="metric-label"],
[data-testid="metric-container"] [data-testid="stMetricLabel"] { font-size: 0.875rem !important; font-weight: normal !important; }

/* 側邊欄文字縮小 */
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stCaption { font-size: 0.75rem !important; }

/* 手機響應式 */
@media (max-width: 640px) {
    .stDataFrame { font-size: 12px !important; }
    .stDataFrame th, .stDataFrame td { padding: 3px 6px !important; }
    .block-container { padding-top: 0.75rem !important; }
    [data-testid="collapsedControl"] { width: 48px !important; height: 48px !important; }
    .stTextInput input { font-size: 16px !important; }
}
</style>
""", unsafe_allow_html=True)

# ── 資料載入 ──────────────────────────────────────────────────
all_dates = get_all_available_dates()
if not all_dates:
    st.error("⚠️ 資料庫目前是空的，請執行同步腳本抓取資料。")
    st.stop()

available_date_objs = {datetime.strptime(d, "%Y%m%d").date() for d in all_dates}
latest_date_obj   = max(available_date_objs)
earliest_date_obj = min(available_date_objs)

# ── 側邊欄：日期選擇 ──────────────────────────────────────────
st.sidebar.header("🗓️ 歷史存檔資料")
st.sidebar.caption("🔵 藍色 = 有資料的日期")

with st.sidebar.expander("📅 有資料的日期一覽", expanded=False):
    st.markdown("\n".join(
        f"- :blue[{datetime.strptime(d, '%Y%m%d').strftime('%Y-%m-%d')}]"
        for d in all_dates
    ))

picked = st.sidebar.date_input(
    "選擇交易日期",
    value=latest_date_obj,
    min_value=earliest_date_obj,
    max_value=latest_date_obj,
    format="YYYY-MM-DD",
)
if picked not in available_date_objs:
    closest = min(available_date_objs, key=lambda d: abs((d - picked).days))
    st.sidebar.warning(f"⚠️ {picked} 無資料，自動切換至 {closest}")
    picked = closest

target_date          = picked.strftime("%Y%m%d")
selected_display_date = picked.strftime("%Y-%m-%d")

st.sidebar.divider()

stock_df = load_all_data(target_date)

# ── 側邊欄：產業篩選 ──────────────────────────────────────────
st.sidebar.subheader("🏭 產業篩選")
all_cats      = sorted(stock_df["產業"].dropna().unique().tolist())
selected_cats = st.sidebar.multiselect("選擇產業（可複選）", all_cats, placeholder="不選 = 全部顯示")

st.sidebar.divider()

# ── 側邊欄：資料管理工具 ──────────────────────────────────────
with st.sidebar.expander("🔧 資料管理工具"):
    render_admin_panel()

# ── 主內容區 ──────────────────────────────────────────────────
st.title(f"台股 — {selected_display_date} 數據報表")

if stock_df.empty:
    st.warning(f"⚠️ {selected_display_date} 尚無資料。")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("顯示家數（已過濾權證）", f"{len(stock_df)} 家")
col2.metric("總成交金額", f"{float(stock_df['成交金額'].sum()) / 1e8:.2f} 億")
col3.metric("查詢日期", selected_display_date)

col_search, col_lookup = st.columns([2, 1])
search_query = col_search.text_input("🔍 搜尋股票代碼或名稱", "")
lookup_id    = col_lookup.text_input("📊 集保分析", placeholder="代碼，例：2330")

if lookup_id:
    sid = lookup_id.strip()
    match = stock_df[stock_df["代碼"] == sid] if not stock_df.empty else pd.DataFrame()
    name  = match.iloc[0]["名稱"] if not match.empty else ""
    label = f"📊 開啟 {sid} {name} 集保分析" if name else f"📊 開啟 {sid} 集保分析"
    if st.button(label, use_container_width=True, key="open_concentration"):
        show_concentration_dialog(sid, name)

display_df = stock_df.copy()
if selected_cats:
    display_df = display_df[display_df["產業"].isin(selected_cats)]
if search_query:
    display_df = display_df[
        display_df["代碼"].str.contains(search_query, na=False) |
        display_df["名稱"].str.contains(search_query, na=False)
    ]


def fmt_change(row):
    d   = str(row.get("漲跌方向") or "").strip()
    val = row.get("漲跌")
    try:
        v = float(val)
    except (TypeError, ValueError):
        return "─"
    if d in ("+", "X"):
        return f"▲ {v:.2f}"
    elif d in ("-", "Y"):
        return f"▼ {v:.2f}"
    return "─" if v == 0 else (f"▲ {v:.2f}" if v > 0 else f"▼ {v:.2f}")


def color_change(val: str):
    if str(val).startswith("▲"):
        return "color:#ff1744; font-weight:bold"
    elif str(val).startswith("▼"):
        return "color:#00c853; font-weight:bold"
    return "color:#9e9e9e"


render_df = display_df.copy()
render_df["漲跌"] = render_df.apply(fmt_change, axis=1)
render_df = render_df.drop(columns=["漲跌方向"])

view_mode = st.radio("顯示模式", ["精簡（手機）", "完整"], horizontal=True, label_visibility="collapsed")
show_df      = render_df[MOBILE_COLS] if view_mode == "精簡（手機）" else render_df
active_fmt   = {k: v for k, v in FMT_MAP.items() if k in show_df.columns}
table_height = 500 if view_mode == "精簡（手機）" else 650

st.dataframe(
    show_df.style
        .map(color_change, subset=["漲跌"])
        .format(active_fmt, na_rep="-"),
    use_container_width=True,
    height=table_height,
)
