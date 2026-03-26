from langchain_ollama import OllamaLLM
import json

# 初始化你家裡的 Llama 3 (RTX 3060 的動力來源)
llm = OllamaLLM(model="llama3")

def analyze_company_ai_value(company_name, business_content):
    prompt = f"""
    你是一位專業的證券分析師。請根據以下公司的「主要業務內容」，分析其在 AI 產業鏈中的定位。
    
    公司名稱：{company_name}
    業務內容：{business_content}
    
    請嚴格依照 JSON 格式回傳結果：
    {{
        "company": "{company_name}",
        "sector": "產業分類(如:半導體、散熱、代工)",
        "ai_relevance": "AI 關聯度 (0.0-1.0)",
        "chain_position": "在 AI 鏈的位置 (如:上游設備、中游封測、下游應用)",
        "investment_note": "50字以內的基本面觀察重點"
    }}
    """
    
    print(f"🧠 AI 正在掃描 {company_name} 的產業價值...")
    response = llm.invoke(prompt)
    
    try:
        # 提取 JSON 部分 (防止 AI 多廢話)
        json_start = response.find('{')
        json_end = response.rfind('}') + 1
        return json.loads(response[json_start:json_end])
    except:
        return {{"error": "AI 分析失敗", "raw": response[:100]}}

# 測試範例
# result = analyze_company_ai_value("台達電", "電源供應器、散熱解決方案、自動化...")
# print(result)