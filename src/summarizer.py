import os
import time
import psycopg2
import cloudscraper
import json
from bs4 import BeautifulSoup
from google import genai
from dotenv import load_dotenv

load_dotenv()

# 1. 初始化資料庫連線
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

# 2. 初始化 Gemini AI Client
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
MODEL_NAME = "gemini-flash-latest"

def get_ai_summary(text):
    prompt = f"""
    請針對以下科技新聞內容進行專業分析：
    1. 提供 3 個重點摘要（繁體中文）。
    2. 判定情緒評分（1-10分，1最悲觀，10最樂觀）。
    3. 判定產業類別（如：AI, 半導體, 雲端服務）。
    
    內容如下：{text[:3000]}
    
    請嚴格按照以下 JSON 格式回覆：
    {{
        "summary": "摘要內容",
        "score": 數字,
        "category": "類別"
    }}
    """
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config={'response_mime_type': 'application/json'}
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"❌ AI 處理失敗: {e}")
        return None

def run_crawler():
    print("🚀 開始採集 iThome 科技新聞...")
    scraper = cloudscraper.create_scraper()
    res = scraper.get("https://www.ithome.com.tw/news")
    soup = BeautifulSoup(res.text, 'html.parser')
    
    # 使用更新後的正確選擇器
    items = soup.find_all("div", class_="views-row")
    print(f"📊 偵測到 {len(items)} 則新聞，開始進行分析...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    for item in items[:5]:
        title_tag = item.find("div", class_="views-field-title")
        if not title_tag: continue
        title_a = title_tag.find("a")
        if not title_a: continue
        
        title = title_a.text.strip()
        link = "https://www.ithome.com.tw" + title_a['href']
        
        # 檢查是否已重複
        cur.execute("SELECT id FROM news_summaries WHERE url = %s", (link,))
        if cur.fetchone(): 
            print(f"⏩ 跳過已存在新聞: {title}")
            continue
        
        print(f"📝 正在分析新聞內文: {title}")
        content_res = scraper.get(link)
        content_soup = BeautifulSoup(content_res.text, 'html.parser')
        article_body = content_soup.select_one(".content-article")
        text = article_body.text if article_body else ""
        
        ai_data = get_ai_summary(text)
        if ai_data:
            cur.execute("""
                INSERT INTO news_summaries (title, url, summary, sentiment_score, category)
                VALUES (%s, %s, %s, %s, %s)
            """, (title, link, ai_data['summary'], ai_data['score'], ai_data['category']))
            conn.commit()
            print(f"✅ 資料已成功存入資料庫")
            time.sleep(2) # 避免觸發 429 配額限制

    cur.close()
    conn.close()
    print("🏁 採集與分析任務完成！")

if __name__ == "__main__":
    run_crawler()