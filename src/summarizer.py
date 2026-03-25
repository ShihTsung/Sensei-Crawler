import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from langchain_ollama import OllamaLLM
from database import init_db, save_summary
from config import SCRAPER_CONFIG, HEADERS

# 1. 初始化資料庫與 AI 模型
init_db()
llm = OllamaLLM(model="llama3")

def fetch_news_content(url):
    """根據 URL 自動匹配網站結構並抓取內容 (進階容錯版)"""
    domain = urlparse(url).netloc
    config = SCRAPER_CONFIG.get(domain)

    if not config:
        print(f"⚠️ 尚未支援此網站的解析規則: {domain}")
        return None

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # 動態建立搜尋參數
        t_args = {"class_": config['title_class']} if config['title_class'] else {}
        c_args = {"class_": config['content_class']} if config['content_class'] else {}

        # 執行抓取
        title_element = soup.find(config['title_tag'], **t_args)
        content_element = soup.find(config['content_tag'], **c_args)

        # 備案：如果找不到標題，直接找網頁唯一的 <h1>
        if not title_element:
            title_element = soup.find('h1')

        if not title_element or not content_element:
            print(f"❌ 找不到元素。網域: {domain}")
            return None

        return {
            "title": title_element.get_text(strip=True),
            "content": content_element.get_text(strip=True)[:2000],
            "company": config['company']
        }
    except Exception as e:
        print(f"❌ 抓取失敗 ({domain}): {e}")
        return None

def analyze_news(news_data):
    """使用 LLM 分析新聞內容"""
    prompt = f"""
    你是一位專業的科技分析師。請分析以下新聞內容，並以 JSON 格式回傳結果。
    新聞標題：{news_data['title']}
    新聞內容：{news_data['content']}
    
    請回傳以下欄位：
    1. title: 繁體中文標題
    2. company: 來源媒體名稱
    3. summary: 三個重點摘要（繁體中文陣列）
    4. sentiment: 正面、中立 或 負面
    
    注意：只回傳 JSON，不要有其他解釋文字。
    """
    try:
        response = llm.invoke(prompt)
        clean_json = response.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"❌ AI 分析失敗: {e}")
        return None

if __name__ == "__main__":
    # 請確保這些網址在當下是有效的
    urls = [
        "https://technews.tw/2026/03/24/nvidia-ceo-jensen-huang-ai-vision/",
        "https://techorange.com/2026/03/24/google-gemini-pro-update/",
        "https://www.bnext.com.tw/article/82100/ai-agent-2026-trend",
        "https://news.nextapple.com/technology/20260324/ai-safety-regulation/"
    ]
    
    print("🚀 Sensei 啟動中...")
    for url in urls:
        print(f"\n--- 🚀 開始處理新任務 ---")
        print(f"🔍 正在抓取: {url}")
        
        news_data = fetch_news_content(url)
        if news_data:
            print(f"🧠 AI 正在思考中... ({news_data['company']})")
            result = analyze_news(news_data)
            if result:
                save_summary(result, url)

    print("\n✅ 所有批次任務執行完畢！")