#!/usr/bin/env bash
# deploy.sh — 部署 Sensei-Crawler 到 Google Cloud Run
#
# 前置作業：
#   1. gcloud auth login
#   2. gcloud config set project YOUR_PROJECT_ID
#   3. gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudscheduler.googleapis.com
#   4. 在 Supabase 建好資料庫，執行 init_db() 的建表 SQL
#
# 用法：
#   bash deploy.sh YOUR_PROJECT_ID

set -euo pipefail

PROJECT_ID="${1:?請傳入 GCP Project ID，例如：bash deploy.sh my-project-123}"
REGION="asia-east1"                       # 台灣最近的 GCP 區域
IMAGE_APP="${REGION}-docker.pkg.dev/${PROJECT_ID}/sensei/sensei-app"
IMAGE_JOBS="${REGION}-docker.pkg.dev/${PROJECT_ID}/sensei/sensei-jobs"

# ── 從 .env 讀取 DB 設定（部署時注入為 Cloud Run 環境變數）──────
if [ ! -f .env ]; then
  echo "❌ 找不到 .env，請先建立（參考 .env.example）"
  exit 1
fi
source .env

echo "======================================================"
echo "  部署至 Project: ${PROJECT_ID}  Region: ${REGION}"
echo "======================================================"

# ── 1. 建置並推送映像 ──────────────────────────────────────────
echo ""
echo "▶ [1/5] 建置 sensei-app 映像..."
gcloud builds submit \
  --tag "${IMAGE_APP}" \
  --timeout=10m \
  .

echo ""
echo "▶ [2/5] 建置 sensei-jobs 映像..."
gcloud builds submit \
  --tag "${IMAGE_JOBS}" \
  --timeout=10m \
  --config=cloudbuild-jobs.yaml \
  --substitutions="_TAG=${IMAGE_JOBS}" \
  .

# ── 2. 部署 Streamlit 主應用（sensei-app）────────────────────
echo ""
echo "▶ [3/5] 部署 sensei-app..."
gcloud run deploy sensei-app \
  --image "${IMAGE_APP}" \
  --region "${REGION}" \
  --platform managed \
  --memory 1Gi \
  --cpu 1 \
  --max-instances 1 \
  --min-instances 0 \
  --timeout 300 \
  --set-env-vars "DB_HOST=${DB_HOST},DB_NAME=${DB_NAME},DB_USER=${DB_USER},DB_PASSWORD=${DB_PASSWORD},DB_PORT=${DB_PORT},GEMINI_API_KEY=${GEMINI_API_KEY},PYTHONPATH=/app/src" \
  --no-allow-unauthenticated

APP_URL=$(gcloud run services describe sensei-app \
  --region "${REGION}" --format "value(status.url)")
echo "   ✅ sensei-app 網址：${APP_URL}"

# ── 3. 部署排程觸發服務（sensei-jobs）────────────────────────
echo ""
echo "▶ [4/5] 部署 sensei-jobs..."
gcloud run deploy sensei-jobs \
  --image "${IMAGE_JOBS}" \
  --region "${REGION}" \
  --platform managed \
  --memory 512Mi \
  --cpu 1 \
  --max-instances 1 \
  --min-instances 0 \
  --timeout 600 \
  --set-env-vars "DB_HOST=${DB_HOST},DB_NAME=${DB_NAME},DB_USER=${DB_USER},DB_PASSWORD=${DB_PASSWORD},DB_PORT=${DB_PORT},GEMINI_API_KEY=${GEMINI_API_KEY},PYTHONPATH=/app/src" \
  --no-allow-unauthenticated

JOBS_URL=$(gcloud run services describe sensei-jobs \
  --region "${REGION}" --format "value(status.url)")
echo "   ✅ sensei-jobs 網址：${JOBS_URL}"

# ── 4. 建立 Cloud Scheduler（盤中 + 集保週資料）──────────────
echo ""
echo "▶ [5/5] 設定 Cloud Scheduler..."

# 取得 Cloud Run 呼叫用的 Service Account
SA="$(gcloud iam service-accounts list \
  --filter="displayName:Compute Engine default" \
  --format="value(email)")"

# 盤中快照：週一至週五 台北時間 9,10,11,12,13 點（UTC 1,2,3,4,5 點）
gcloud scheduler jobs create http sensei-intraday \
  --location "${REGION}" \
  --schedule "0 1,2,3,4,5 * * 1-5" \
  --time-zone "UTC" \
  --uri "${JOBS_URL}/intraday" \
  --http-method POST \
  --oidc-service-account-email "${SA}" \
  --oidc-token-audience "${JOBS_URL}" \
  2>/dev/null || \
gcloud scheduler jobs update http sensei-intraday \
  --location "${REGION}" \
  --schedule "0 1,2,3,4,5 * * 1-5" \
  --uri "${JOBS_URL}/intraday" \
  --oidc-service-account-email "${SA}"

echo "   ✅ 盤中快照排程：週一至週五 09-13 點（台北時間）"

# 集保週資料：每週五台北時間 18:00（UTC 10:00）
gcloud scheduler jobs create http sensei-tdcc \
  --location "${REGION}" \
  --schedule "0 10 * * 5" \
  --time-zone "UTC" \
  --uri "${JOBS_URL}/tdcc" \
  --http-method POST \
  --oidc-service-account-email "${SA}" \
  --oidc-token-audience "${JOBS_URL}" \
  2>/dev/null || \
gcloud scheduler jobs update http sensei-tdcc \
  --location "${REGION}" \
  --schedule "0 10 * * 5" \
  --uri "${JOBS_URL}/tdcc" \
  --oidc-service-account-email "${SA}"

echo "   ✅ 集保週資料排程：每週五 18:00（台北時間）"

# 每日行情：週一至週五台北時間 16:00（UTC 8:00，收盤後）
gcloud scheduler jobs create http sensei-prices \
  --location "${REGION}" \
  --schedule "0 8 * * 1-5" \
  --time-zone "UTC" \
  --uri "${JOBS_URL}/prices" \
  --http-method POST \
  --oidc-service-account-email "${SA}" \
  --oidc-token-audience "${JOBS_URL}" \
  2>/dev/null || \
gcloud scheduler jobs update http sensei-prices \
  --location "${REGION}" \
  --schedule "0 8 * * 1-5" \
  --uri "${JOBS_URL}/prices" \
  --oidc-service-account-email "${SA}"

echo "   ✅ 每日行情排程：週一至週五 16:00（台北時間）"

echo ""
echo "======================================================"
echo "  部署完成！"
echo "  Streamlit 儀表板：${APP_URL}"
echo ""
echo "  iPhone 存取方式："
echo "  1. 安裝 gcloud CLI app (或用下方指令建立代理)"
echo "  2. gcloud run services proxy sensei-app --region ${REGION}"
echo "     → 開啟 http://localhost:8080"
echo ""
echo "  或設定 Cloud IAP + 自訂網域以直接從 iPhone 存取"
echo "======================================================"
