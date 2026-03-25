import psycopg2
from datetime import datetime

# 資料庫連線配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "2rligaoi",  # <-- 這裡請修改
    "port": "5432"
}

def init_db():
    """初始化資料表"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 建立資料表 (使用 TEXT[] 存儲摘要陣列)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_summaries (
                id SERIAL PRIMARY KEY,
                title TEXT,
                company TEXT,
                summary TEXT[],
                sentiment TEXT,
                url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ PostgreSQL 資料表檢查/初始化成功")
    except Exception as e:
        print(f"❌ 資料庫初始化失敗: {e}")

def save_summary(data, url):
    """將 AI 結果存入資料庫"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO news_summaries (title, company, summary, sentiment, url)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            data.get('title'),
            data.get('company'),
            data.get('summary'), # psycopg2 會自動處理 Python list 轉 Postgres Array
            data.get('sentiment'),
            url
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("💾 [Success] 資料已存入 PostgreSQL！")
    except Exception as e:
        print(f"❌ 寫入資料庫失敗: {e}")

import json
import trafilatura
from langchain_ollama import OllamaLLM
from database import init_db, save_summary  # 確保 database.py 在同一個資料夾

# --- 1. 定義函式 (這是你剛才噴錯遺失的部分) ---
def sensei_analyze(url):
    print(f"🔍 正在抓取網頁: {url}")
    downloaded = trafilatura.fetch_url(url)
    content = trafilatura.extract(downloaded)
    
    if not content:
        return {"error": "無法抓取網頁內容"}

    # 設定 Ollama (Llama 3)
    llm = OllamaLLM(model="llama3", format="json")
    
    prompt = f"""
    請針對以下文章內容進行摘要，並以 JSON 格式回傳。
    格式要求：
    {{
        "title": "文章標題",
        "company": "相關公司名稱",
        "summary": ["三句話總結重點1", "三句話總結重點2", "三句話總結重點3"],
        "sentiment": "正面/中立/負面"
    }}
    
    文章內容：
    {content[:3000]}
    """
    
    print("🧠 AI 正在思考中... (這可能需要 10-30 秒)")
    ai_response = llm.invoke(prompt)
    
    try:
        return json.loads(ai_response)
    except Exception as e:
        return {"error": f"JSON 解析失敗: {str(e)}", "raw": ai_response}

# --- 2. 執行進入點 ---
if __name__ == "__main__":
    print("🚀 Sensei 啟動中...") 
    
    # 初始化資料庫 (PostgreSQL)
    init_db()
    urls = [
        "https://www.inside.com.tw/article/34524-nvidia-blackwell-b200",
        "https://www.inside.com.tw/article/34533-openai-sora-hollywood",
        "https://www.inside.com.tw/article/34510-microsoft-must-not-be-allowed-to-monopolize-ai"
    ]
    
    for url in urls:
        print(f"\n--- 🚀 開始處理新任務 ---")
        result = sensei_analyze(url)
        if result and "error" not in result:
            save_summary(result, url)
        else:
            print(f"❌ 略過失敗任務: {url}")
            
    print("\n✅ 所有批次任務執行完畢！請去 pgAdmin 重新整理查看。")
    # 呼叫剛才定義的函式
    