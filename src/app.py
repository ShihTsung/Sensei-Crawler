import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from data.queries import get_all_available_dates, load_all_data
from ui.constants import FMT_MAP, MOBILE_COLS
from ui.dialogs import show_concentration_dialog
from sync_shareholding import sync_insider_holding
from import_categories import import_from_df
from sync_company_info import sync_company_info
from sync_top10 import sync_top10
from twse_historical_sync import sync_historical
from sync_range import run_sync_for_date
from sync_tdcc import sync_tdcc_weekly

st.set_page_config(layout="wide", page_title="台股戰略終端")
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

# ── 側邊欄：個股集保查詢 ──────────────────────────────────────
st.sidebar.subheader("🔎 個股集保分析")
lookup_id = st.sidebar.text_input("輸入股票代碼", placeholder="例如：2330")

stock_df = load_all_data(target_date)

if lookup_id:
    sid   = lookup_id.strip()
    match = stock_df[stock_df["代碼"] == sid] if not stock_df.empty else pd.DataFrame()
    name  = match.iloc[0]["名稱"] if not match.empty else ""
    label = f"📊 {sid} {name} 集保分析" if name else f"📊 {sid} 集保分析"
    if st.sidebar.button(label, use_container_width=True):
        show_concentration_dialog(sid, name)

st.sidebar.divider()

# ── 側邊欄：產業篩選 ──────────────────────────────────────────
st.sidebar.subheader("🏭 產業篩選")
all_cats      = sorted(stock_df["產業"].dropna().unique().tolist())
selected_cats = st.sidebar.multiselect("選擇產業（可複選）", all_cats, placeholder="不選 = 全部顯示")

st.sidebar.divider()

# ── 側邊欄：資料管理工具 ──────────────────────────────────────
with st.sidebar.expander("🔧 資料管理工具"):
    st.markdown("**📂 產業分類更新**")
    st.caption("上傳從證交所下載的產業分類 CSV（需含欄位：代號、公司名稱、新產業類別）")
    uploaded_csv = st.file_uploader("選擇 CSV 檔", type="csv", key="cat_csv")
    if uploaded_csv:
        if st.button("匯入產業分類", use_container_width=True, key="cat_run"):
            try:
                count = import_from_df(pd.read_csv(uploaded_csv))
                st.success(f"✅ 匯入完成：{count} 筆")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ 匯入失敗：{e}")

    st.divider()
    st.markdown("**🏢 公司基本資料**")
    st.caption("一次拉取全市場統編、地址、董事長、電話（建議每年更新一次）")
    if st.button("更新公司基本資料", use_container_width=True, key="ci_run"):
        with st.status("抓取公司基本資料中…", expanded=True) as job:
            try:
                count = sync_company_info()
                st.cache_data.clear()
                job.update(label=f"✅ 完成：{count} 筆", state="complete")
            except Exception as e:
                job.update(label="❌ 失敗", state="error")
                st.error(str(e))

    st.divider()
    st.markdown("**👥 前十大股東（每季）**")
    st.caption("⚠️ 全市場 1700+ 支，約需 30~60 分鐘。部署到 Cloud Run 後請改用排程觸發，避免 HTTP 逾時。")
    now_t10    = datetime.now()
    col_t1, col_t2 = st.columns(2)
    t10_year   = col_t1.number_input("年份", min_value=2015, max_value=now_t10.year, value=now_t10.year, step=1, key="t10_y")
    t10_season = col_t2.selectbox("季度", [1, 2, 3, 4], index=(now_t10.month - 1) // 3, key="t10_s")
    if st.button("🚀 開始抓取", use_container_width=True, key="t10_run"):
        prog_t10   = st.progress(0)
        status_t10 = st.empty()
        def _t10_cb(done, total):
            prog_t10.progress(done / total)
            status_t10.caption(f"{done} / {total} 支")
        with st.status(f"抓取 {t10_year}Q{t10_season} 前十大股東…", expanded=True) as job:
            try:
                count = sync_top10(t10_year, t10_season, progress_cb=_t10_cb)
                st.cache_data.clear()
                job.update(label=f"✅ 完成：{count} 筆", state="complete")
            except Exception as e:
                job.update(label="❌ 失敗", state="error")
                st.error(str(e))

    st.divider()
    st.markdown("**📅 董監持股補齊**")
    now = datetime.now()
    col_y1, col_m1 = st.columns(2)
    col_y2, col_m2 = st.columns(2)
    start_year  = col_y1.number_input("起始年", min_value=2015, max_value=now.year, value=now.year - 1, step=1, key="ih_sy")
    start_month = col_m1.number_input("起始月", min_value=1, max_value=12, value=1, step=1, key="ih_sm")
    end_year    = col_y2.number_input("結束年", min_value=2015, max_value=now.year, value=now.year, step=1, key="ih_ey")
    end_month   = col_m2.number_input("結束月", min_value=1, max_value=12, value=now.month, step=1, key="ih_em")
    if st.button("🚀 開始補齊", use_container_width=True, key="ih_run"):
        months, y, m = [], start_year, start_month
        while (y, m) <= (end_year, end_month):
            months.append((y, m))
            m += 1
            if m > 12:
                m, y = 1, y + 1
        if not months:
            st.warning("起始日期不能晚於結束日期。")
        else:
            prog = st.progress(0)
            with st.status(f"補齊 {len(months)} 個月份…", expanded=True) as job:
                for i, (y, m) in enumerate(months):
                    st.write(f"📡 {y}年{m:02d}月")
                    sync_insider_holding(y, m)
                    prog.progress((i + 1) / len(months))
                job.update(label=f"✅ 完成，共補 {len(months)} 個月", state="complete")

    st.divider()
    st.markdown("**📊 集保週資料**")
    st.caption("從集保中心抓取最新一週的持股分散資料")
    if st.button("同步集保週資料", use_container_width=True, key="tdcc_run"):
        with st.status("同步集保週資料中…", expanded=True) as job:
            try:
                sync_tdcc_weekly()
                st.cache_data.clear()
                job.update(label="✅ 完成", state="complete")
            except Exception as e:
                job.update(label="❌ 失敗", state="error")
                st.error(str(e))

    st.divider()
    st.markdown("**📈 行情＆籌碼同步**")
    st.caption("從證交所抓取收盤行情與三大法人籌碼")
    if st.button("同步最新交易日", use_container_width=True, key="hist_latest"):
        with st.status("同步最新交易日中…", expanded=True) as job:
            try:
                sync_historical()
                st.cache_data.clear()
                job.update(label="✅ 完成", state="complete")
            except Exception as e:
                job.update(label="❌ 失敗", state="error")
                st.error(str(e))

    st.caption("補抓歷史區間（每交易日約需 8 秒，半年約 17 分鐘）")
    now_sync = datetime.now()
    qc1, qc2, qc3 = st.columns(3)
    if qc1.button("1 個月", use_container_width=True, key="qs_1m"):
        st.session_state["sync_start"] = now_sync.date() - timedelta(days=30)
        st.session_state["sync_end"]   = now_sync.date()
    if qc2.button("3 個月", use_container_width=True, key="qs_3m"):
        st.session_state["sync_start"] = now_sync.date() - timedelta(days=90)
        st.session_state["sync_end"]   = now_sync.date()
    if qc3.button("6 個月", use_container_width=True, key="qs_6m"):
        st.session_state["sync_start"] = now_sync.date() - timedelta(days=180)
        st.session_state["sync_end"]   = now_sync.date()
    col_s1, col_s2 = st.columns(2)
    sync_start = col_s1.date_input("起始日", value=now_sync.date() - timedelta(days=7), key="sync_start")
    sync_end   = col_s2.date_input("結束日", value=now_sync.date(), key="sync_end")
    if st.button("🚀 補抓區間", use_container_width=True, key="hist_range"):
        dates, d = [], sync_start
        while d <= sync_end:
            if d.weekday() < 5:
                dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        if not dates:
            st.warning("區間內無交易日。")
        else:
            prog = st.progress(0)
            with st.status(f"補抓 {len(dates)} 個交易日…", expanded=True) as job:
                ok, skip = 0, 0
                for i, ds in enumerate(dates):
                    st.write(f"📡 {ds}")
                    if run_sync_for_date(ds):
                        ok += 1
                    else:
                        skip += 1
                    prog.progress((i + 1) / len(dates))
                st.cache_data.clear()
                job.update(label=f"✅ 完成：{ok} 日成功，{skip} 日跳過", state="complete")

# ── 主內容區 ──────────────────────────────────────────────────
st.title(f"台股 — {selected_display_date} 數據報表")

if stock_df.empty:
    st.warning(f"⚠️ {selected_display_date} 尚無資料。")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("顯示家數（已過濾權證）", f"{len(stock_df)} 家")
col2.metric("總成交金額", f"{float(stock_df['成交金額'].sum()) / 1e8:.2f} 億")
col3.metric("查詢日期", selected_display_date)

search_query = st.text_input("🔍 搜尋股票代碼或名稱", "")
display_df = stock_df.copy()
if selected_cats:
    display_df = display_df[display_df["產業"].isin(selected_cats)]
if search_query:
    display_df = display_df[
        display_df["代碼"].str.contains(search_query, na=False) |
        display_df["名稱"].str.contains(search_query, na=False)
    ]

st.caption("💡 左側輸入股票代碼可查看集保持股分析")


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
        return "color:#00c853; font-weight:bold"
    elif str(val).startswith("▼"):
        return "color:#ff1744; font-weight:bold"
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
