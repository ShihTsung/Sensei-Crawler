import subprocess
import time
import sys

def run_task(name, command):
    print(f"\n{'='*50}")
    print(f"🚀 正在啟動任務: {name}")
    print(f"{'='*50}")
    
    try:
        # 使用 sys.executable 確保使用虛擬環境中的 python
        result = subprocess.run([sys.executable] + command.split(), check=True)
        print(f"✅ {name} 執行成功！")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {name} 執行失敗。錯誤碼: {e.returncode}")
        return False

def main():
    start_time = time.time()
    
    # 定義執行順序 (地基 -> 裝潢 -> 實時數據)
    tasks = [
        ("1. 接入上市名單", "src/init_all_markets.py"),
        ("2. 補全上櫃/興櫃名單", "src/patch_markets.py"),
        ("3. AI 產業權重分析", "src/ai_analyzer.py"),
        ("4. 同步最新行情股價", "src/price_sync.py")
    ]
    
    success_count = 0
    for name, cmd in tasks:
        if run_task(name, cmd):
            success_count += 1
        # 任務間稍作停頓，保護 IaaS 資料庫連線
        time.sleep(1)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    
    print(f"\n{'*'*50}")
    print(f"🎉 所有工作已依序啟動完成！")
    print(f"📊 成功任務: {success_count}/{len(tasks)}")
    print(f"⏱️ 總耗時: {duration} 秒")
    print(f"💡 現在請回到 Streamlit 網頁按下 'R' 重新整理即可。")
    print(f"{'*'*50}")

if __name__ == "__main__":
    main()