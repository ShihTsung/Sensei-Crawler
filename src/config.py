# src/config.py

SCRAPER_CONFIG = {
    "techorange.com": {
        "title_tag": "h1",
        "title_class": "entry-title",
        "content_tag": "div",
        "content_class": "entry-content",
        "company": "TechOrange 科技報橘"
    },
    "technews.tw": {
        "title_tag": "h1",
        "title_class": "entry-title",
        "content_tag": "div",
        "content_class": "indent",
        "company": "Technews 科技新報"
    },
    "www.bnext.com.tw": {
        "title_tag": "h1",
        "title_class": None,
        "content_tag": "div",
        "content_class": "content",
        "company": "數位時代 Bnext"
    },
    "news.nextapple.com": {
        "title_tag": "h1",
        "title_class": "post-title",
        "content_tag": "div",
        "content_class": "post-content",
        "company": "壹蘋新聞網"
    }
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9"
}