# Sensei-Crawler → Google Cloud SQL 遷移指南

> 目標：4–5 個專案（TriagePilot、Sensei-Crawler、rgenda、gpu-cloud-platform）共用**一個** Cloud SQL 實例，
> 各專案 **獨立 DB + 獨立 user**，CloudRun + GCS，連線寫法統一。本檔聚焦 Sensei-Crawler，最後附「可共用到其他專案」的範本。

---

## 背景 / 為什麼幾乎零程式碼變動

- 全專案只有 **一個連線進入點** `src/database.py` 的 `_get_pool()`，所有 sync/queries 都走 `get_connection()`。
- 連線參數全是 env 驅動的離散欄位：`DB_HOST / DB_NAME / DB_USER / DB_PASSWORD / DB_PORT`。
- psycopg2 的 `host` 只要是 `/` 開頭就自動走 **unix socket**；Cloud Run 連 Cloud SQL 的官方做法正是掛載
  `/cloudsql/<INSTANCE_CONNECTION_NAME>`。→ 只要把 `DB_HOST` 設成 socket 路徑，**database.py 一行都不用改**。

---

## 一、GCP 一次性設定（指令）

```bash
REGION=asia-east1
# 1) 建共用實例（多專案共用，Postgres 15）
gcloud sql instances create shared-pg \
  --database-version=POSTGRES_15 --tier=db-g1-small \
  --region=$REGION --storage-size=10GB --storage-auto-increase

# 2) Sensei 專用 DB + user
gcloud sql databases create sensei_db --instance=shared-pg
gcloud sql users create sensei_user --instance=shared-pg --password='<挑一組強密碼>'

# 3) 取得連線名稱（部署時會用到）
gcloud sql instances describe shared-pg --format='value(connectionName)'
#   形如  my-proj:asia-east1:shared-pg
```

> ⚠️ **DB 隔離**：Postgres 預設任何 user 都能連任何 DB、看 public schema。要真隔離，連進每個 DB 後執行：
> `REVOKE CONNECT ON DATABASE sensei_db FROM PUBLIC;` 再 `GRANT CONNECT ... TO sensei_user;`
> 並把各專案的 owner 設成自己的 user。

## 二、密碼進 Secret Manager（取代 deploy.sh 明文 env）

```bash
printf '<同上強密碼>' | gcloud secrets create sensei-db-password --data-file=-
# 授權 Cloud Run 預設 SA 讀取
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
gcloud secrets add-iam-policy-binding sensei-db-password \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role=roles/secretmanager.secretAccessor
```

## 三、`deploy.sh` 變更（兩個 `gcloud run deploy` 各加幾行）

對 **sensei-app** 與 **sensei-jobs** 兩段 `gcloud run deploy` 都加：

```bash
ICN="<上面拿到的 connectionName>"   # my-proj:asia-east1:shared-pg

  --add-cloudsql-instances "${ICN}" \
  --set-env-vars "DB_HOST=/cloudsql/${ICN},DB_NAME=sensei_db,DB_USER=sensei_user,DB_PORT=5432,DB_MAX_CONN=3" \
  --set-secrets "DB_PASSWORD=sensei-db-password:latest" \
```

> 把原本 `--set-env-vars "DB_HOST=...,DB_PASSWORD=..."` 那段換成上面這組（密碼改走 `--set-secrets`）。

## 四、本機 `.env`（開發維持不變）

本機照舊指向 docker-compose 的 `db` / localhost；只有雲端用 socket。零程式碼差異：

```
DB_HOST=db          # 本機 docker-compose；雲端才是 /cloudsql/<ICN>
DB_NAME=sensei_db
DB_USER=postgres
DB_PASSWORD=***
DB_PORT=5432
DB_MAX_CONN=3
```

## 五、建表（一次性）

新實例是空的，跑一次 `init_db()`（程式已有，`src/database.py:63`，且 `__main__` 會呼叫）：

```bash
# 本機開 Cloud SQL Auth Proxy 指向新實例，再跑
cloud-sql-proxy "$ICN" &           # 監聽 127.0.0.1:5432
DB_HOST=127.0.0.1 DB_NAME=sensei_db DB_USER=sensei_user DB_PASSWORD=*** \
  python src/database.py
```

## 六、⚠️ 連線數上限（多專案共用的唯一真風險）

- `database.py` pool `maxconn` 預設 **10**（`DB_MAX_CONN`）。
- `db-g1-small` 的 `max_connections` 約 **數十條**；4–5 專案 × 10 = 40–50 會逼近上限。
- **對策**：每專案 `DB_MAX_CONN=3~5`（上面已設 3），或實例升一級。
- Cloud Run `--max-instances 1` 也讓單服務連線數可控（現況就是 1）。

---

## 七、資料遷移清單 — 哪張表「帶過去」、哪張表「重抓」

Sensei 是純爬蟲，多數資料源頭可重抓；**只有源頭不提供歷史的才需要 `pg_dump` 帶走。**

### 🔴 必須 DUMP（源頭抓不回歷史 / 重生成要花錢）

| 資料表 | 原因 |
|--------|------|
| `twse_weekly_concentration` | 集保週資料，`sync_tdcc.py` 只抓**最新一週**，歷史週回溯不到 |
| `twse_intraday` | 盤中即時快照，過去時點**無法重抓**（排程當下才有） |
| `news_summaries` | AI 生成摘要，且新聞來源可能已消失 |

### 🟡 建議 DUMP（可重生成，但省時 / 省 AI 費用）

| 資料表 | 原因 |
|--------|------|
| `companies` | `ai_relevance`、`ai_analysis_note` 兩欄是 Gemini 生成，重跑要花 API 費用 |
| `stock_category` | 產業分類，靠 CSV 重匯；小表，直接帶省得再找檔 |

### 🟢 直接重抓（源頭有歷史，建空表即可）

| 資料表 | 回補方式 |
|--------|----------|
| `twse_prices` | 行情＆籌碼 →「補抓區間」 |
| `twse_institutional` | 同上（同一流程） |
| `twse_insider_holding` | 「董監持股補齊」(月區間) |
| `twse_top10_shareholders` | 「前十大股東」逐季（全市場每季約 30–60 分） |
| `company_info` | 「更新公司基本資料」按鈕（全市場一次） |

### DUMP / RESTORE 指令範本

```bash
# 從 Supabase 只 dump 要保留的表（data-only）
pg_dump "$SUPABASE_CONN" --data-only --column-inserts \
  -t twse_weekly_concentration -t twse_intraday -t news_summaries \
  -t companies -t stock_category \
  > sensei_keep.sql

# 經 Auth Proxy restore 進 Cloud SQL
psql "host=127.0.0.1 port=5432 dbname=sensei_db user=sensei_user" < sensei_keep.sql
```

> ⚠️ **回補很慢 + Cloud Run 300 秒逾時**：大量歷史靠網頁按鈕會逾時，要用排程服務 **sensei-jobs** 慢慢跑
> （UI 自己有警告：每交易日約 8 秒、半年約 17 分；前十大股東每季 30–60 分）。

---

## 八、執行順序（搬移當天）

1. 建實例 + DB + user（§一）→ Secret（§二）
2. 改 `deploy.sh`（§三）
3. 對新實例跑 `init_db()` 建表（§五）
4. `pg_dump` 紅/黃清單的表 → restore（§七）
5. `bash deploy.sh <PROJECT_ID>` 部署
6. 綠清單資料：手動按鈕補近期 + 讓排程逐步回補歷史
7. 驗證：開 dashboard 確認近日有資料、查一支股票集保分析正常 → 確認舊 Supabase 可下線

---

## 九、共用到其他專案的範本

把 §一~§三 的模式抽成各專案共用約定：
- **同一實例 `shared-pg`**，每專案一組 `xxx_db` + `xxx_user`（獨立、REVOKE PUBLIC）。
- 連線一律走 **env + unix socket**（`DB_HOST=/cloudsql/<ICN>`），密碼一律 Secret Manager。
- 每專案 `DB_MAX_CONN` 小（3–5）。
- 各專案 `deploy.sh` 的 `gcloud run deploy` 統一加 `--add-cloudsql-instances` + `--set-secrets`。

> 這樣 4–5 個專案連線方式完全一致，之後不用每次改連線寫法。

---

### 交付方式
此文件將存成 **`Sensei-Crawler/CLOUD_SQL_MIGRATION.md`**（內層 canonical repo 根目錄），方便屆時搬移時直接照做。
