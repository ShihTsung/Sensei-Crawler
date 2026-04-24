import os
import pandas as pd
from google import genai

_client: genai.Client | None = None


def _get_client() -> genai.Client:
    global _client
    if _client is None:
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY 未設定")
        _client = genai.Client(api_key=api_key)
    return _client


def analyze_concentration(
    stock_id: str,
    stock_name: str,
    group_pivot: pd.DataFrame,
) -> str:
    latest = group_pivot.iloc[-1]
    n = min(4, len(group_pivot))
    delta_df = group_pivot.diff().dropna().tail(n)

    trend_lines = []
    for date, row in delta_df.iterrows():
        parts = "、".join(f"{g} {v:+.2f}%" for g, v in row.items())
        trend_lines.append(f"  {date}：{parts}")
    trend_text = "\n".join(trend_lines) if trend_lines else "（資料不足）"

    prompt = f"""你是台股籌碼分析師。請分析 {stock_id}（{stock_name}）的集保持股分散資料：

最新持股比例：
- 大戶（400張以上）：{latest.get('大戶', 0):.1f}%
- 中實戶（10–400張）：{latest.get('中實戶', 0):.1f}%
- 散戶（10張以下）：{latest.get('散戶', 0):.1f}%

近期週變化趨勢：
{trend_text}

請以繁體中文，用 4–6 句話解讀籌碼動向，最後給出「偏多」「偏空」或「中性」判斷，並說明理由。"""

    response = _get_client().models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return response.text
