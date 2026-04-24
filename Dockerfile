FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run 使用 $PORT（預設 8080），本機 Docker 也可覆蓋
EXPOSE 8080
ENV PORT=8080
ENV PYTHONPATH=/app/src

CMD ["sh", "-c", "python -m streamlit run src/app.py \
  --server.port=${PORT} \
  --server.address=0.0.0.0 \
  --server.headless=true"]
