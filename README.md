# 台股戰略終端 Sensei-Crawler

台股資料爬蟲 + Streamlit 儀表板，整合行情、籌碼、集保、董監持股、前十大股東，並接入 Gemini AI 進行產業分析。

## 技術棧

| 層級 | 技術 |
|------|------|
| 前端 | Streamlit |
| 資料庫 | PostgreSQL（地端 Homebrew，多專案共用同一實例） |
| AI | Google Gemini (`google-genai`) |
| 部署 | 地端原生執行（雲端為 GCP Cloud Run，見 `CLOUD_SQL_MIGRATION.md`） |
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

## 快速啟動（地端，不使用 Docker）

資料庫使用**本機 Homebrew PostgreSQL**（與其他專案共用同一實例，各自獨立 DB + user）。

### 1. 安裝並啟動 PostgreSQL

```bash
brew install postgresql@17
brew services start postgresql@17
```

### 2. 建立本專案專用資料庫與使用者（只需一次）

```bash
psql -d postgres <<'SQL'
CREATE ROLE sensei_user LOGIN PASSWORD 'sensei_dev_2026';
CREATE DATABASE sensei_crawler OWNER sensei_user ENCODING 'UTF8';
REVOKE CONNECT ON DATABASE sensei_crawler FROM PUBLIC;   -- 與其他專案隔離
GRANT ALL ON DATABASE sensei_crawler TO sensei_user;
SQL
```

### 3. 設定環境變數

```bash
git clone https://github.com/ShihTsung/Sensei-Crawler.git
cd Sensei-Crawler
cp .env.example .env   # 填入 DB_PASSWORD 與 GEMINI_API_KEY
```

### 4. 啟動

```bash
./run.sh init    # 首次：建表（idempotent）
./run.sh app     # 啟動儀表板 → http://127.0.0.1:8503
```

首次啟動後，匯入產業分類（只需執行一次）：

```bash
PYTHONPATH=src .venv/bin/python src/import_categories.py
```

盤中快照（選用，需要常駐時）：

```bash
./run.sh intraday
```

> `run.sh` 首次執行會自動以 Python 3.13 建立 `.venv` 並安裝 `requirements.txt`。

## 環境變數

| 變數 | 說明 | 地端值 |
|------|------|--------|
| `DB_HOST` | 資料庫主機 | `localhost` |
| `DB_NAME` | 資料庫名稱 | `sensei_crawler` |
| `DB_USER` | 使用者 | `sensei_user` |
| `DB_PASSWORD` | 密碼 | 自訂（地端 `trust` 模式可留空） |
| `DB_PORT` | 埠號 | `5432` |
| `DB_MAX_CONN` | 連線池上限 | `5`（多專案共用宜小） |
| `GEMINI_API_KEY` | Google Gemini API Key | 必填 |

## 主要腳本

| 腳本 | 用途 |
|------|------|
| `src/twse_historical_sync.py` | 同步最近一個交易日行情＋籌碼 |
| `src/sync_range.py` | 補抓指定日期區間 |
| `src/sync_tdcc.py` | 集保週資料同步 |
| `src/sync_shareholding.py` | 董監持股同步 |
| `src/sync_top10.py` | 前十大股東同步 |
| `src/sync_company_info.py` | 公司基本資料同步 |
| `src/intraday_sync.py` | 盤中快照（`./run.sh intraday` 常駐執行） |

## 資料備份／還原

資料存於本機 PostgreSQL 的 `sensei_crawler` 資料庫。

```bash
# 備份
pg_dump -U sensei_user sensei_crawler > backup.sql

# 還原
psql -U sensei_user -d sensei_crawler < backup.sql
```
