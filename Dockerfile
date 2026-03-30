FROM python:3.11-slim

WORKDIR /app

# 安裝必要的 Linux 底層工具
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 複製並安裝套件
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 複製程式碼
COPY . .

# 暴露 Streamlit 埠口
EXPOSE 8501

# 確保使用 python -m streamlit 來執行，這最不容易出錯
CMD ["python", "-m", "streamlit", "run", "src/app.py", "--server.port=8501", "--server.address=0.0.0.0"]