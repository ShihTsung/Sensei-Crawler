import cloudscraper
import json
import feedparser
import re  # 新增：用於強力清洗 JSON
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from langchain_ollama import OllamaLLM
from database import init_db, save_summary
from config import SCRAPER_CONFIG, HEADERS

# 1. 初始化環境
init_db()
# 建議增加 timeout 或調整參數確保穩定
llm = OllamaLLM(model="llama3", temperature=0) 
scraper = cloudscraper.create_scraper()

def fetch_news_content(url):
    """強效抓取內文 (支援子網域匹配、避開 403 並限制字數減輕 CPU 負擔)"""
    parsed_netloc = urlparse(url).netloc
    config_key = next((k for k in SCRAPER_CONFIG if k in parsed_netloc), None)
    config = SCRAPER_CONFIG.get(config_key)
    
    if not config:
        return None

    try:
        response = scraper.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200: return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        content_div = soup.find(config['content_tag'], class_=config['content_class'])
        
        if content_div:
            # 限制字數，避免模型跑太久且減少解析出錯率
            return content_div.get_text(strip=True)[:2500]
    except Exception as e:
        print(f"❌ 抓取內文失敗: {e}")
    return None

def parse_ai_response(response):
    """強力 JSON 解析器：排除 Llama 3 的開場白與結尾"""
    try:
        # 尋找第一個 { 與最後一個 }
        start = response.find('{')
        end = response.rfind('}') + 1
        if start == -1 or end == 0:
            print(f"DEBUG: AI 回傳找不到 JSON 結構")
            return None
            
        json_str = response[start:end]
        # 移除換行符號與非法控制字元
        json_str = re.sub(r'[\x00-\x1F\x7F]', '', json_str)
        
        return json.loads(json_str, strict=False)
    except Exception as e:
        print(f"❌ AI 分析 JSON 失敗: {e}")
        return None

def analyze_news(content, provider):
    """呼叫 Llama 3 進行摘要"""
    if not content: return None
    
    # 嚴格的 Prompt 要求
    prompt = f"""
    You are a professional tech analyst. Analyze the following news content and return ONLY a valid JSON object.
    Do NOT include any preamble, markdown code blocks, or explanations.

    Target JSON structure:
    {{
        "title": "新聞標題",
        "company": "{provider}",
        "summary": ["重點一", "重點二", "重點三"],
        "sentiment": "正面/負面/中立"
    }}

    News Content:
    {content}
    """
    
    try:
        response = llm.invoke(prompt)
        return parse_ai_response(response)
    except Exception as e:
        print(f"❌ Llama 3 呼叫失敗: {e}")
        return None

if __name__ == "__main__":
    rss_feeds = {
        "https://technews.tw/feed/": "科技新報",
        "https://techorange.com/feed/": "科技報橘",
        "https://www.bnext.com.tw/rss": "數位時代"
    }

    print("🚀 Sensei 智慧監測系統 (Llama 3 強化版) 啟動...")
    
    for rss_url, provider in rss_feeds.items():
        print(f"\n--- 🚀 正在從 {provider} 獲取最新內容 ---")
        feed = feedparser.parse(rss_url)
        # 抓取前 10 則
        latest_entries = feed.entries[:10]
        
        for i, entry in enumerate(latest_entries, 1):
            target_url = entry.link
            print(f"[{i}/10] 🔗 處理中: {target_url}")
            
            content = fetch_news_content(target_url)
            if content:
                print(f"🧠 AI 分析中... (Llama 3)")
                result = analyze_news(content, provider)
                if result:
                    save_summary(result, target_url)
                else:
                    print(f"⚠️ 跳過：分析失敗")
            else:
                print(f"⚠️ 跳過：無法取得內文")