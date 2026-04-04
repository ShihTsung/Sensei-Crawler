import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from database import get_connection
from datetime import datetime
from sync_shareholding import sync_insider_holding
from import_categories import import_from_df
from sync_company_info import sync_company_info
from sync_top10 import sync_top10

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
                p.stock_id    AS "代碼",
                p.stock_name  AS "名稱",
                sc.category_name AS "產業",
                p.open_price  AS "開盤",
                p.high_price  AS "最高",
                p.low_price   AS "最低",
                p.close_price      AS "收盤",
                p.price_change_dir AS "漲跌方向",
                p.price_change     AS "漲跌",
                p.pe_ratio         AS "本益比",
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
            LEFT JOIN stock_category sc
                   ON p.stock_id = sc.stock_id
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

# ── 持股分組定義 ─────────────────────────────────────────────
GROUPS = {
    "散戶": list(range(1, 6)),       # level 1–5
    "中實戶": list(range(6, 11)),    # level 6–10
    "大戶": list(range(11, 16)),     # level 11–15
}
GROUP_COLORS = {"散戶": "#4C9BE8", "中實戶": "#F4A261", "大戶": "#E63946"}

_PLOTLY_LAYOUT = dict(
    margin=dict(l=0, r=0, t=30, b=0),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)

# ── 集保詳細 Dialog ───────────────────────────────────────────

@st.dialog("📊 集保持股分析", width="large")
def show_concentration_dialog(stock_id: str, stock_name: str):
    st.markdown(f"### {stock_id}　{stock_name}")

    df = load_concentration(stock_id)
    if df.empty:
        st.warning("資料庫尚無此股票的集保週資料。")
        return

    dates = sorted(df["date"].unique(), reverse=True)
    latest = dates[0]
    n_weeks = len(dates)

    # 分組 pivot：date x group，值 = 各組 rate 加總
    group_pivot = pd.DataFrame({
        gname: df[df["level"].isin(levels)].groupby("date")["rate"].sum()
        for gname, levels in GROUPS.items()
    }).sort_index()

    # ── 頂部指標：三組週環比 ──────────────────────────────────
    col1, col2, col3 = st.columns(3)
    for col, gname in zip([col1, col2, col3], GROUPS):
        curr = float(group_pivot[gname].iloc[-1])
        delta = float(group_pivot[gname].iloc[-1] - group_pivot[gname].iloc[-2]) if n_weeks >= 2 else None
        col.metric(gname, f"{curr:.1f} %", f"{delta:+.2f} %" if delta is not None else None)

    st.caption(f"共 {n_weeks} 週歷史資料　最新：{latest}")
    st.divider()

    tab1, tab2 = st.tabs(["📈 週變化趨勢", "🍩 最新持股分布"])

    # ── Tab 1：週變化量 grouped bar ───────────────────────────
    with tab1:
        st.caption("正值 = 該族群本週持股佔比增加（買進）　負值 = 減少（賣出）")
        if n_weeks < 2:
            st.info("需要至少 2 週資料才能計算變化量。")
        else:
            delta_df = group_pivot.diff().dropna()
            fig = go.Figure()
            for gname, color in GROUP_COLORS.items():
                fig.add_trace(go.Bar(
                    x=delta_df.index,
                    y=delta_df[gname].round(2),
                    name=gname,
                    marker_color=color,
                    hovertemplate="%{x}<br><b>%{y:+.2f} %</b><extra>" + gname + "</extra>",
                ))
            fig.add_hline(y=0, line_color="rgba(150,150,150,0.5)", line_width=1)
            fig.update_layout(
                **_PLOTLY_LAYOUT,
                barmode="group",
                height=360,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                yaxis=dict(title="佔比週變化 %", gridcolor="rgba(128,128,128,0.15)"),
                xaxis=dict(gridcolor="rgba(128,128,128,0.1)"),
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Tab 2：Donut + 15 級橫向 bar ─────────────────────────
    with tab2:
        st.caption(f"資料日期：{latest}")

        # Donut：三大分組
        latest_vals = [float(group_pivot[g].iloc[-1]) for g in GROUPS]
        fig_donut = go.Figure(go.Pie(
            labels=list(GROUPS.keys()),
            values=[round(v, 2) for v in latest_vals],
            hole=0.52,
            textinfo="label+percent",
            textfont_size=13,
            marker=dict(colors=list(GROUP_COLORS.values())),
            hovertemplate="%{label}<br>%{value:.2f} %<extra></extra>",
        ))
        fig_donut.update_layout(
            **_PLOTLY_LAYOUT,
            height=260,
            showlegend=False,
        )
        st.plotly_chart(fig_donut, use_container_width=True)

        # 15 級詳細橫向 bar（可折疊，手機不佔版面）
        with st.expander("各級詳細分布"):
            latest_detail = (
                df[df["date"] == latest]
                .sort_values("level")
                .assign(持股區間=lambda x: x["level"].map(LEVEL_LABELS))
            )
            fig_bar = go.Figure(go.Bar(
                x=latest_detail["rate"].round(2),
                y=latest_detail["持股區間"],
                orientation="h",
                marker_color="#4C9BE8",
                hovertemplate="%{y}<br>%{x:.2f} %<extra></extra>",
            ))
            fig_bar.update_layout(
                **_PLOTLY_LAYOUT,
                height=400,
                xaxis=dict(title="佔比 %", gridcolor="rgba(128,128,128,0.15)"),
                yaxis=dict(gridcolor="rgba(128,128,128,0.1)"),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

# ── 主畫面 ────────────────────────────────────────────────────

all_dates = get_all_available_dates()

if not all_dates:
    st.error("⚠️ 資料庫目前是空的，請執行同步腳本抓取資料。")
    st.stop()

# 把有資料的日期轉成 date 物件集合，供提示使用
available_date_objs = {
    datetime.strptime(d, "%Y%m%d").date() for d in all_dates
}
latest_date_obj = max(available_date_objs)
earliest_date_obj = min(available_date_objs)

# ── 側邊欄 ────────────────────────────────────────────────────
st.sidebar.header("🗓️ 歷史存檔資料")

# 有資料的日期標示提示（顯示在日曆上方）
st.sidebar.caption("🔵 藍色 = 有資料的日期")

# 用 CSS 把有資料的日期文字染藍（Streamlit 日曆原生不支援格色，
# 改用 markdown 列出有資料的日期範圍提示，讓使用者知道哪些有資料）
with st.sidebar.expander("📅 有資料的日期一覽", expanded=False):
    date_list_md = "\n".join(
        f"- :blue[{datetime.strptime(d, '%Y%m%d').strftime('%Y-%m-%d')}]"
        for d in all_dates
    )
    st.markdown(date_list_md)

# 日曆選擇器（date_input）
picked = st.sidebar.date_input(
    "選擇交易日期",
    value=latest_date_obj,
    min_value=earliest_date_obj,
    max_value=latest_date_obj,
    format="YYYY-MM-DD",
)

# 如果選到沒有資料的日期，自動跳到最近有資料的日期
if picked not in available_date_objs:
    # 找最近的有資料日期
    closest = min(available_date_objs, key=lambda d: abs((d - picked).days))
    st.sidebar.warning(f"⚠️ {picked} 無資料，自動切換至 {closest}")
    picked = closest

target_date = picked.strftime("%Y%m%d")
selected_display_date = picked.strftime("%Y-%m-%d")

st.sidebar.divider()

# ── 個股集保查詢（側邊欄）────────────────────────────────────
st.sidebar.subheader("🔎 個股集保分析")
lookup_id = st.sidebar.text_input("輸入股票代碼", placeholder="例如：2330")

# 載入主資料（先載入，才能查股票名稱）
stock_df = load_all_data(target_date)

if lookup_id:
    sid = lookup_id.strip()
    match = stock_df[stock_df["代碼"] == sid] if not stock_df.empty else pd.DataFrame()
    name = match.iloc[0]["名稱"] if not match.empty else ""
    label = f"📊 {sid} {name} 集保分析" if name else f"📊 {sid} 集保分析"
    if st.sidebar.button(label, use_container_width=True):
        show_concentration_dialog(sid, name)

st.sidebar.divider()

# ── 產業篩選 ─────────────────────────────────────────────────
st.sidebar.subheader("🏭 產業篩選")
all_cats = sorted(stock_df["產業"].dropna().unique().tolist())
selected_cats = st.sidebar.multiselect(
    "選擇產業（可複選）", all_cats, placeholder="不選 = 全部顯示"
)

st.sidebar.divider()

# ── 資料管理工具 ──────────────────────────────────────────────
with st.sidebar.expander("🔧 資料管理工具"):
    # ── 產業分類 CSV 匯入 ─────────────────────────────────────
    st.markdown("**📂 產業分類更新**")
    st.caption("上傳從證交所下載的產業分類 CSV（需含欄位：代號、公司名稱、新產業類別）")
    uploaded_csv = st.file_uploader("選擇 CSV 檔", type="csv", key="cat_csv")
    if uploaded_csv:
        if st.button("匯入產業分類", use_container_width=True, key="cat_run"):
            try:
                df_csv = pd.read_csv(uploaded_csv)
                count = import_from_df(df_csv)
                st.success(f"✅ 匯入完成：{count} 筆")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"❌ 匯入失敗：{e}")

    st.divider()

    # ── 公司基本資料 ──────────────────────────────────────────
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

    # ── 前十大股東 ────────────────────────────────────────────
    st.markdown("**👥 前十大股東（每季）**")
    st.caption("⚠️ 全市場 1700+ 支，約需 30~60 分鐘，請耐心等候")
    now_t10 = datetime.now()
    col_t1, col_t2 = st.columns(2)
    t10_year   = col_t1.number_input("年份", min_value=2015, max_value=now_t10.year,
                                     value=now_t10.year, step=1, key="t10_y")
    t10_season = col_t2.selectbox("季度", [1, 2, 3, 4],
                                  index=(now_t10.month - 1) // 3, key="t10_s")

    if st.button("🚀 開始抓取", use_container_width=True, key="t10_run"):
        prog_t10 = st.progress(0)
        status_t10 = st.empty()

        def _t10_progress(done, total):
            prog_t10.progress(done / total)
            status_t10.caption(f"{done} / {total} 支")

        with st.status(f"抓取 {t10_year}Q{t10_season} 前十大股東…", expanded=True) as job:
            try:
                count = sync_top10(t10_year, t10_season, progress_cb=_t10_progress)
                st.cache_data.clear()
                job.update(label=f"✅ 完成：{count} 筆", state="complete")
            except Exception as e:
                job.update(label="❌ 失敗", state="error")
                st.error(str(e))

    st.divider()

    # ── 董監持股補齊 ──────────────────────────────────────────
    st.markdown("**📅 董監持股補齊**")
    now = datetime.now()
    col_y1, col_m1 = st.columns(2)
    start_year  = col_y1.number_input("起始年", min_value=2015, max_value=now.year, value=now.year - 1, step=1, key="ih_sy")
    start_month = col_m1.number_input("起始月", min_value=1, max_value=12, value=1, step=1, key="ih_sm")
    col_y2, col_m2 = st.columns(2)
    end_year  = col_y2.number_input("結束年", min_value=2015, max_value=now.year, value=now.year, step=1, key="ih_ey")
    end_month = col_m2.number_input("結束月", min_value=1, max_value=12, value=now.month, step=1, key="ih_em")

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

# ── 主內容區 ──────────────────────────────────────────────────
st.title(f"台股 — {selected_display_date} 數據報表")

if stock_df.empty:
    st.warning(f"⚠️ {selected_display_date} 尚無資料。")
    st.stop()

# 儀表板
col1, col2, col3 = st.columns(3)
col1.metric("顯示家數（已過濾權證）", f"{len(stock_df)} 家")
col2.metric("總成交金額", f"{stock_df['成交金額'].sum() / 1e8:.2f} 億")
col3.metric("查詢日期", selected_display_date)

# 搜尋 + 產業篩選
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

# ── 漲跌符號格式化 ─────────────────────────────────────────────
def fmt_change(row):
    """將 price_change_dir + price_change 組合成 ▲/▼/─ 顯示"""
    d   = str(row.get("漲跌方向") or "").strip()
    val = row.get("漲跌")
    try:
        v = float(val)
    except (TypeError, ValueError):
        return "─"
    if d in ("+", "X"):          # 上漲 / 漲停
        return f"▲ {v:.2f}"
    elif d in ("-", "Y"):        # 下跌 / 跌停
        return f"▼ {v:.2f}"
    else:
        return f"─" if v == 0 else (f"▲ {v:.2f}" if v > 0 else f"▼ {v:.2f}")

def color_change(val: str):
    if str(val).startswith("▲"):
        return "color:#00c853; font-weight:bold"
    elif str(val).startswith("▼"):
        return "color:#ff1744; font-weight:bold"
    return "color:#9e9e9e"

render_df = display_df.copy()
render_df["漲跌"] = render_df.apply(fmt_change, axis=1)
render_df = render_df.drop(columns=["漲跌方向"])

st.dataframe(
    render_df.style
        .map(color_change, subset=["漲跌"])
        .format({
            "收盤": "{:.2f}", "開盤": "{:.2f}", "最高": "{:.2f}", "最低": "{:.2f}",
            "本益比": "{:.2f}",
            "成交量": "{:,}", "成交金額": "{:,.0f}",
        }, na_rep="-"),
    use_container_width=True,
    height=650,
)
