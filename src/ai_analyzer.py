import sys
import os
from database import get_connection

# 根據證交所代碼定義 AI 權重 (針對 '24', '25' 等代碼)
# 24:半導體, 25:電腦週邊, 27:通信網路, 28:電子通路, 30:電子零組件, 31:其他電子
AI_CODE_WEIGHTS = {
    "24": {"score": 0.95, "label": "核心半導體 (AI晶片)"},
    "25": {"score": 0.85, "label": "AI伺服器與週邊"},
    "31": {"score": 0.75, "label": "AI硬體零組件 (機殼/散熱)"},
    "27": {"score": 0.70, "label": "網通基礎設施 (CPO/交換器)"},
    "28": {"score": 0.60, "label": "電子通路與代理"},
    "30": {"score": 0.65, "label": "電子基礎元件 (PCB)"}
}

def analyze():
    print("🧠 啟動代碼權重分析引擎...")
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT stock_id, company_name, industry FROM companies")
                companies = cur.fetchall()
                print(f"📊 掃描中... 總數: {len(companies)}")

                updated = 0
                for stock_id, name, code in companies:
                    # 去除可能的多餘空格
                    code = str(code).strip()
                    
                    if code in AI_CODE_WEIGHTS:
                        config = AI_CODE_WEIGHTS[code]
                        score = config["score"]
                        note = config["label"]
                        
                        # 更新資料庫
                        cur.execute("""
                            UPDATE companies 
                            SET ai_relevance = %s, ai_analysis_note = %s
                            WHERE stock_id = %s
                        """, (score, note, stock_id))
                        updated += 1
                
                conn.commit()
                print(f"✅ 分析完成！已根據產業代碼精準標記 {updated} 家企業。")
                
    except Exception as e:
        print(f"❌ 錯誤: {e}")

if __name__ == "__main__":
    analyze()