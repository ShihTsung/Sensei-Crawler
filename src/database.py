import psycopg2
import os
import socket
from dotenv import load_dotenv
from functools import wraps

def auto_env_config(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 1. 優先加載 .env 檔案
        load_dotenv()
        
        # 2. 自動判斷是否在 Docker 容器內
        # Docker 容器內通常會存在 /.dockerenv 檔案
        if os.path.exists('/.dockerenv'):
            os.environ["DB_HOST"] = "db" # 容器內強制指向 compose 中的服務名稱
        else:
            # 3. 如果在本地 (Windows/Mac)，根據 hostname 載入特定配置
            hostname = socket.gethostname()
            if "PeterChendeMac-mini" in hostname or "PeterMacBook-Air" in hostname:
                load_dotenv(".env.mac", override=True)
            else:
                load_dotenv(".env.windows", override=True)
                
        return func(*args, **kwargs)
    return wrapper

@auto_env_config
def get_connection():
    """建立資料庫連線"""
    return psycopg2.connect(
        # 優先讀取環境變數，若無則預設為 localhost
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "sensei_db"), # 配合您的 docker-compose 設定
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "2rligaoi"), # 配合您的 docker-compose 設定
        port=os.getenv("DB_PORT", "5432")
    )