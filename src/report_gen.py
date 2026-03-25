import psycopg2
from datetime import date  # 確保這行有在
from database import DB_CONFIG

def generate_daily_report():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. 撈出資料
        cursor.execute('''
            SELECT title, company, summary, sentiment, url 
            FROM news_summaries 
            ORDER BY created_at DESC 
            LIMIT 5
        ''')
        rows = cursor.fetchall()
        
        # 2. 組合內容
        report = f"# 🚀 Sensei 今日科技情報總結 ({date.today()})\n\n" # 標題也加上日期
        
        for row in rows:
            title, company, summary_list, sentiment, url = row
            report += f"## 📰 {title}\n"
            report += f"- **來源**: {company}\n"
            report += f"- **情緒**: {sentiment}\n"
            report += f"- **核心重點**:\n"
            for point in summary_list:
                report += f"  - {point}\n"
            report += f"- [🔗 原文連結]({url})\n\n"
            report += "---\n\n"
            
        # --- 核心修正點：這裡要用 f-string 把日期帶入檔名 ---
        filename = f"Daily_Report_{date.today()}.md" 
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(report)
            
        print(f"📊 報告已生成：{filename}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ 報告生成失敗: {e}")

if __name__ == "__main__":
    generate_daily_report()