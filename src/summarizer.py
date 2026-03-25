import cloudscraper
import json
import feedparser
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from langchain_ollama import OllamaLLM
from database import init_db, save_summary
from config import SCRAPER_CONFIG, HEADERS

# 1. 初始化環境
init_db()
llm = OllamaLLM(model="llama3")
scraper = cloudscraper.create_scraper()

def get_latest_news_from_rss(rss_url):
    """使用 RSS 獲取最新一則新聞連結"""
    try:
        feed = feedparser.parse(rss_url)
        if feed.entries:
            return feed.entries[0].link
    except Exception as e:
        print(f"⚠️ RSS 讀取失敗 ({rss_url}): {e}")
    return None

def fetch_news_content(url):
    """強效抓取內文 (支援子網域匹配、避開 403 並限制字數減輕 CPU 負擔)"""
    parsed_netloc = urlparse(url).netloc
    
    # 智慧匹配網域規則
    config_key = next((k for k in SCRAPER_CONFIG if k in parsed_netloc), None)
    config = SCRAPER_CONFIG.get(config_key)
    
    if not config:
        print(f"⚠️ 找不到 {parsed_netloc} 的解析規則")
        return None

    try:
        response = scraper.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # 標題抓取 (含容錯)
        t_args = {"class_": config['title_class']} if config['title_class'] else {}
        title_element = soup.find(config['title_tag'], **t_args) or soup.find('h1')

        # 內文抓取 (含容錯備案)
        c_args = {"class_": config['content_class']} if config['content_class'] else {}
        content_element = soup.find(config['content_tag'], **c_args)
        
        if not content_element:
            # 針對 TechOrange 等網站的 article 備案
            content_element = soup.find('article') or soup.find('div', class_='entry-content') or soup.find('div', class_='post-content')

        if not title_element or not content_element:
            return None

        return {
            "title": title_element.get_text(strip=True),
            "content": content_element.get_text(strip=True)[:600], # 💡 限制 500 字，保護 CPU 
            "company": config['company']
        }
    except Exception as e:
        print(f"❌ 抓取失敗 ({parsed_netloc}): {e}")
        return None

def analyze_news(news_data):
    """AI 分析並強制提取 JSON 物件 (增加 Log 除錯)"""
    prompt = f"""
    你是一位專業的科技分析師。請分析以下新聞內容並嚴格以 JSON 格式回傳。
    標題：{news_data['title']}
    內容：{news_data['content']}
    
    回傳格式：
    {{
        "title": "繁體中文標題",
        "company": "{news_data['company']}",
        "summary": ["重點1", "重點2", "重點3"],
        "sentiment": "正面/中立/負面"
    }}
    注意：不要有任何開場白，只需回傳 JSON。
    """
    try:
        response = llm.invoke(prompt)
        # 強制擷取 JSON 部分
        start = response.find('{')
        end = response.rfind('}') + 1
        
        if start == -1 or end == 0:
            print(f"DEBUG: AI 回傳不是 JSON -> {response[:100]}")
            return None
            
        return json.loads(response[start:end])
    except Exception as e:
        print(f"❌ AI 分析 JSON 失敗: {e}")
        return None

if __name__ == "__main__":
    rss_feeds = {
        "https://technews.tw/feed/": "科技新報",
        "https://techorange.com/feed/": "科技報橘",
        "https://www.bnext.com.tw/rss": "數位時代"
    }

    print("🚀 Sensei 智慧監測系統 (10 則批次模式) 啟動...")
    
    for rss_url, provider in rss_feeds.items():
        print(f"\n--- 🚀 正在從 {provider} 獲取最新 10 則 ---")
        
        feed = feedparser.parse(rss_url)
        # 💡 使用 [:10] 抓取前 10 則，如果不夠 10 則會抓取全部
        latest_entries = feed.entries[:10]
        
        print(f"📡 發現 {len(latest_entries)} 則新聞，準備開始批次分析...")

        for i, entry in enumerate(latest_entries, 1):
            target_url = entry.link
            print(f"[{i}/10] 🔗 處理中: {target_url}")
            
            # 1. 抓取內文
            news_data = fetch_news_content(target_url)
            
            if news_data:
                print(f"🧠 AI 分析中... (這可能需要一點時間)")
                # 2. AI 分析
                result = analyze_news(news_data)
                if result:
                    # 3. 存入資料庫 (save_summary 內建 URL 唯一性檢查，不會重複存)
                    save_summary(result, target_url)
            else:
                print(f"❌ 無法抓取內容，跳過。")

    print("\n✅ 今日所有批次情報採集任務已完成！")