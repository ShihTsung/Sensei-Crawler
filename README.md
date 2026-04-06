# 台股戰略終端 Sensei-Crawler

台股資料爬蟲 + Streamlit 儀表板，整合行情、籌碼、集保、董監持股、前十大股東，並接入 Gemini AI 進行產業分析。

## 技術棧

| 層級 | 技術 |
|------|------|
| 前端 | Streamlit |
| 資料庫 | PostgreSQL 14（Docker volume 持久化） |
| AI | Google Gemini (`google-genai`) |
| 部署 | Docker Compose |
| 資料來源 | 證交所 TWSE、集保 TDCC、公開資訊觀測站 MOPS |

## 功能

- **行情總覽**：每日收盤行情（含開高低收、成交量、本益比）
- **三大法人籌碼**：外資、投信、自營商買賣超
- **集保持股分析**：散戶／中實戶／大戶週變化趨勢與分布圖
- **董監持股**：歷史月份補抓
- **前十大股東**：季度資料（全市場約需 30~60 分鐘）
- **公司基本資料**：統編、地址、董事長、電話
- **產業篩選**：依產業類別過濾個股
- **歷史行情同步**：UI 內一鍵同步最新交易日或補抓指定區間

## 快速啟動

### 環境準備

```bash
git clone https://github.com/ShihTsung/Sensei-Crawler.git
cd Sensei-Crawler
cp .env.example .env
```

編輯 `.env`，填入密碼與 API Key：

```env
DB_HOST=db          # Docker 環境固定填 db
DB_PASSWORD=你的密碼
GEMINI_API_KEY=你的金鑰
```

### 啟動

```bash
docker compose up -d
```

首次啟動後，匯入產業分類（只需執行一次）：

```bash
docker exec sensei-app python src/import_categories.py
```

瀏覽器開啟 **http://localhost:8503**

### 本機 Mac 直接執行（不用 Docker）

```env
# .env 改為
DB_HOST=localhost
```

```bash
pip install -r requirements.txt
streamlit run src/app.py
```

## 環境變數

| 變數 | 說明 | Docker | Mac 本機 |
|------|------|--------|----------|
| `DB_HOST` | 資料庫主機 | `db` | `localhost` |
| `DB_NAME` | 資料庫名稱 | `sensei_db` | `sensei_db` |
| `DB_USER` | 使用者 | `postgres` | `postgres` |
| `DB_PASSWORD` | 密碼 | 自訂 | 自訂 |
| `DB_PORT` | 埠號 | `5432` | `5432` |
| `GEMINI_API_KEY` | Google Gemini API Key | 必填 | 必填 |

## 主要腳本

| 腳本 | 用途 |
|------|------|
| `src/twse_historical_sync.py` | 同步最近一個交易日行情＋籌碼 |
| `src/sync_range.py` | 補抓指定日期區間 |
| `src/sync_tdcc.py` | 集保週資料同步 |
| `src/sync_shareholding.py` | 董監持股同步 |
| `src/sync_top10.py` | 前十大股東同步 |
| `src/sync_company_info.py` | 公司基本資料同步 |
| `src/intraday_sync.py` | 盤中快照（Docker 背景執行） |

## 資料持久化

PostgreSQL 資料存於 Docker named volume `postgres_data`，container 重啟或重建不影響資料。

```bash
# 備份
docker exec sensei-db pg_dump -U postgres sensei_db > backup.sql

# 還原
docker exec -i sensei-db psql -U postgres sensei_db < backup.sql
```
