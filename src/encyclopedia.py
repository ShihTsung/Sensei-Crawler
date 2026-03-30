import os
import json
from google import genai
from dotenv import load_dotenv

load_dotenv()

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
MODEL_NAME = "gemini-flash-latest"

def get_company_profile(company_name):
    prompt = f"""
    你現在是一位資深的 IaaS 與 PaaS 架構師。請針對「{company_name}」這家公司進行企業百科建模。
    請提供以下資訊：
    1. 核心業務描述。
    2. 針對該公司目前規模，建議的技術架構（如：Hybrid Cloud, NVIDIA BasePOD 等）。
    3. SWOT 分析（優勢、劣勢、機會、威脅）。
    
    請嚴格使用 JSON 格式回覆，格式如下：
    {{
        "core_business": "...",
        "tech_stack_advice": "...",
        "swot": {{
            "strengths": [],
            "weaknesses": [],
            "opportunities": [],
            "threats": []
        }}
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
        print(f"❌ 百科檢索失敗: {e}")
        return None