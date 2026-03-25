# src/config.py

SCRAPER_CONFIG = {
    "technews.tw": {
        "title_tag": "h1",
        "title_class": "entry-title",
        "content_tag": "div",
        "content_class": "indent",
        "company": "科技新報"
    },
    "techorange.com": {
        "title_tag": "h1",
        "title_class": "entry-title",
        "content_tag": "div",
        "content_class": "entry-content", # 或是 "post-content"
        "company": "科技報橘"
    },
    "bnext.com.tw": {
        "title_tag": "h1",
        "title_class": None,
        "content_tag": "div",
        "content_class": "content",
        "company": "數位時代"
    }
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}