import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.queries import load_concentration
from ui.constants import GROUP_COLORS, GROUPS, LEVEL_LABELS, PLOTLY_LAYOUT


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

    group_pivot = pd.DataFrame({
        gname: df[df["level"].isin(levels)].groupby("date")["rate"].sum()
        for gname, levels in GROUPS.items()
    }).sort_index()

    col1, col2, col3 = st.columns(3)
    for col, gname in zip([col1, col2, col3], GROUPS):
        curr = float(group_pivot[gname].iloc[-1])
        delta = float(group_pivot[gname].iloc[-1] - group_pivot[gname].iloc[-2]) if n_weeks >= 2 else None
        col.metric(gname, f"{curr:.1f} %", f"{delta:+.2f} %" if delta is not None else None)

    st.caption(f"共 {n_weeks} 週歷史資料　最新：{latest}")
    st.divider()

    tab1, tab2 = st.tabs(["📈 週變化趨勢", "🍩 最新持股分布"])

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
                **PLOTLY_LAYOUT,
                barmode="group",
                height=360,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                yaxis=dict(title="佔比週變化 %", gridcolor="rgba(128,128,128,0.15)"),
                xaxis=dict(gridcolor="rgba(128,128,128,0.1)"),
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.caption(f"資料日期：{latest}")

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
        fig_donut.update_layout(**PLOTLY_LAYOUT, height=260, showlegend=False)
        st.plotly_chart(fig_donut, use_container_width=True)

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
                **PLOTLY_LAYOUT,
                height=400,
                xaxis=dict(title="佔比 %", gridcolor="rgba(128,128,128,0.15)"),
                yaxis=dict(gridcolor="rgba(128,128,128,0.1)"),
            )
            st.plotly_chart(fig_bar, use_container_width=True)
