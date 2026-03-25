import psycopg2
from datetime import date
from database import DB_CONFIG 

def generate_daily_report():
    try:
        # 1. 連線資料庫
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 2. 撈出最新的 5 筆摘要資料
        print("📊 正在從 PostgreSQL 讀取最新情報...")
        cursor.execute('''
            SELECT title, company, summary, sentiment, url, created_at 
            FROM news_summaries 
            ORDER BY created_at DESC 
            LIMIT 5
        ''')
        rows = cursor.fetchall()
        
        if not rows:
            print("⚠️ 資料庫中目前沒有資料喔！")
            return

        # 3. 組合 Markdown 內容 (在記憶體中處理字串)
        report = "# 🚀 Sensei AI 科技情報日報\n"
        report += f"生成時間：{rows[0][5].strftime('%Y-%m-%d %H:%M')}\n\n"
        report += "--- \n\n"
        
        for row in rows:
            title, company, summary_list, sentiment, url, created_at = row
            emoji = "🟢" if sentiment == "正面" else "⚪" if sentiment == "中立" else "🔴"
            
            report += f"## {emoji} {title}\n"
            report += f"- **來源**: `{company}`\n"
            report += f"- **AI 判定**: {sentiment}\n"
            report += f"- **核心重點摘要**:\n"
            for point in summary_list:
                report += f"  - {point}\n"
            report += f"- [🔗 閱讀原文]({url})\n\n"
            report += "---\n\n"
            
        # 4. 寫入檔案 (放在迴圈外面，只寫入一次)
        filename = f"Daily_Report_{date.today()}.md"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(report)
            
        print(f"✨ 報告已成功生成：{filename}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ 報告生成失敗: {e}")

if __name__ == "__main__":
    generate_daily_report()