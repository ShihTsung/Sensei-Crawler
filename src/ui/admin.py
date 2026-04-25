"""
資料管理工具：所有同步、匯入按鈕集中於此模組。

每個區塊獨立成一個 `_section_xxx()` 函式，方便日後新增功能：
  1. 加 import
  2. 寫一個新的 _section_xxx()
  3. 在 render_admin_panel() 中接上即可
"""

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from import_categories import import_from_df
from sync_company_info import sync_company_info
from sync_range import run_sync_for_date
from sync_shareholding import sync_insider_holding
from sync_tdcc import sync_tdcc_weekly
from sync_top10 import sync_top10
from twse_historical_sync import sync_historical


# ── 入口 ──────────────────────────────────────────────────────

def render_admin_panel() -> None:
    """在 sidebar expander 內呈現完整資料管理面板。"""
    sections = [
        _section_category_upload,
        _section_company_info,
        _section_top10,
        _section_insider_holding,
        _section_tdcc,
        _section_historical,
    ]
    for i, section in enumerate(sections):
        if i > 0:
            st.divider()
        section()


# ── 區塊：產業分類 CSV 匯入 ──────────────────────────────────

def _section_category_upload() -> None:
    st.markdown("**📂 產業分類更新**")
    st.caption("上傳從證交所下載的產業分類 CSV（需含欄位：代號、公司名稱、新產業類別）")
    uploaded_csv = st.file_uploader("選擇 CSV 檔", type="csv", key="cat_csv")
    if uploaded_csv and st.button("匯入產業分類", use_container_width=True, key="cat_run"):
        try:
            count = import_from_df(pd.read_csv(uploaded_csv))
            st.success(f"✅ 匯入完成：{count} 筆")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"❌ 匯入失敗：{e}")


# ── 區塊：公司基本資料 ────────────────────────────────────────

def _section_company_info() -> None:
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


# ── 區塊：前十大股東 ──────────────────────────────────────────

def _section_top10() -> None:
    st.markdown("**👥 前十大股東（每季）**")
    st.caption("⚠️ 全市場 1700+ 支，約需 30~60 分鐘。部署到 Cloud Run 後請改用排程觸發，避免 HTTP 逾時。")
    now = datetime.now()
    c1, c2 = st.columns(2)
    year   = c1.number_input("年份", min_value=2015, max_value=now.year, value=now.year, step=1, key="t10_y")
    season = c2.selectbox("季度", [1, 2, 3, 4], index=(now.month - 1) // 3, key="t10_s")

    if st.button("🚀 開始抓取", use_container_width=True, key="t10_run"):
        prog   = st.progress(0)
        status = st.empty()

        def _cb(done: int, total: int) -> None:
            prog.progress(done / total)
            status.caption(f"{done} / {total} 支")

        with st.status(f"抓取 {year}Q{season} 前十大股東…", expanded=True) as job:
            try:
                count = sync_top10(year, season, progress_cb=_cb)
                st.cache_data.clear()
                job.update(label=f"✅ 完成：{count} 筆", state="complete")
            except Exception as e:
                job.update(label="❌ 失敗", state="error")
                st.error(str(e))


# ── 區塊：董監持股補齊 ────────────────────────────────────────

def _section_insider_holding() -> None:
    st.markdown("**📅 董監持股補齊**")
    now = datetime.now()
    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)
    sy = c1.number_input("起始年", min_value=2015, max_value=now.year, value=now.year - 1, step=1, key="ih_sy")
    sm = c2.number_input("起始月", min_value=1, max_value=12, value=1, step=1, key="ih_sm")
    ey = c3.number_input("結束年", min_value=2015, max_value=now.year, value=now.year, step=1, key="ih_ey")
    em = c4.number_input("結束月", min_value=1, max_value=12, value=now.month, step=1, key="ih_em")

    if not st.button("🚀 開始補齊", use_container_width=True, key="ih_run"):
        return

    months, y, m = [], sy, sm
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1

    if not months:
        st.warning("起始日期不能晚於結束日期。")
        return

    prog = st.progress(0)
    with st.status(f"補齊 {len(months)} 個月份…", expanded=True) as job:
        for i, (y, m) in enumerate(months):
            st.write(f"📡 {y}年{m:02d}月")
            sync_insider_holding(y, m)
            prog.progress((i + 1) / len(months))
        job.update(label=f"✅ 完成，共補 {len(months)} 個月", state="complete")


# ── 區塊：集保週資料 ──────────────────────────────────────────

def _section_tdcc() -> None:
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


# ── 區塊：行情＆籌碼同步 ──────────────────────────────────────

def _section_historical() -> None:
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
    today = datetime.now().date()
    qc1, qc2, qc3 = st.columns(3)
    if qc1.button("1 個月", use_container_width=True, key="qs_1m"):
        st.session_state["sync_start"] = today - timedelta(days=30)
        st.session_state["sync_end"]   = today
    if qc2.button("3 個月", use_container_width=True, key="qs_3m"):
        st.session_state["sync_start"] = today - timedelta(days=90)
        st.session_state["sync_end"]   = today
    if qc3.button("6 個月", use_container_width=True, key="qs_6m"):
        st.session_state["sync_start"] = today - timedelta(days=180)
        st.session_state["sync_end"]   = today

    c1, c2 = st.columns(2)
    sync_start = c1.date_input("起始日", value=today - timedelta(days=7), key="sync_start")
    sync_end   = c2.date_input("結束日", value=today, key="sync_end")

    if not st.button("🚀 補抓區間", use_container_width=True, key="hist_range"):
        return

    dates, d = [], sync_start
    while d <= sync_end:
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)

    if not dates:
        st.warning("區間內無交易日。")
        return

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
