import subprocess
import sys
import os
from datetime import datetime

def run_news_flow():
    # 取得專案根目錄路徑
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    summarizer = os.path.join(base_path, "src", "summarizer.py")
    report_gen = os.path.join(base_path, "src", "report_gen.py")

    print(f"\n{'='*50}")
    print(f"📡 Sensei 科技情報監測啟動 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}")

    try:
        # 1. 執行 AI 摘要分析
        print("\nStep 1: 正在執行 Llama 3 新聞爬取與分析...")
        subprocess.run([sys.executable, summarizer], check=True)
        
        # 2. 執行報表生成
        print("\nStep 2: 正在生成今日 MD 戰略日報...")
        subprocess.run([sys.executable, report_gen], check=True)
        
        print(f"\n{'*'*50}")
        print(f"✅ 情報監測任務完成！")
        print(f"📝 日報已產出，請查看專案根目錄。")
        print(f"🌐 請重新整理 Streamlit Dashboard 查看最新動態。")
        print(f"{'*'*50}")

    except subprocess.CalledProcessError as e:
        print(f"\n❌ 執行過程中出錯，錯誤碼: {e.returncode}")

if __name__ == "__main__":
    run_news_flow()