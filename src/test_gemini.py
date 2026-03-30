import os
import sys
import io
import time
from google import genai
from dotenv import load_dotenv

# 解決編碼
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def final_test():
    # 根據你的清單，嘗試這三個最有希望的型號
    models_to_try = ['gemini-2.0-flash', 'gemini-flash-latest', 'gemini-2.0-flash-lite-001']
    
    for model_name in models_to_try:
        print(f"📡 正在嘗試連線型號: {model_name} ...")
        try:
            # 增加一個重試邏輯，避免 429
            for i in range(3):
                try:
                    response = client.models.generate_content(
                        model=model_name,
                        contents="請說出：Sensei，大腦最終連線成功！"
                    )
                    print(f"🤖 [{model_name}] AI 回覆：{response.text}")
                    print(f"✅ 成功！請記住這個型號：{model_name}")
                    return
                except Exception as e:
                    if "429" in str(e):
                        print(f"⏳ 觸發配額限制，等待 10 秒後重試 ({i+1}/3)...")
                        time.sleep(10)
                    else:
                        raise e
        except Exception as e:
            print(f"❌ {model_name} 失敗：{str(e)[:100]}...")

if __name__ == "__main__":
    final_test()