# 🚀 Sensei-Crawler AI 產業情報站

這是一個自動化 AI 爬蟲系統，能抓取科技新聞、進行 LLM 摘要，並將資料存入 PostgreSQL 與生成報表。

## 🛠️ 技術棧 (Stack)
- **Database**: PostgreSQL 17.9
- **AI Model**: Ollama / Llama 3
- **Frontend**: Streamlit Dashboard

## 📂 快速啟動 (Quick Start)
1. 啟動環境: `.\venv\Scripts\Activate.ps1`
2. 執行抓取: `python src/summarizer.py`
3. 生成報告: `python src/report_gen.py`
4. 網頁展示: `streamlit run src/app.py`