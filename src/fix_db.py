import sys
import os
sys.path.append(os.path.dirname(__file__))
from database import get_connection

def fix_missing_table():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS twse_institutional_investors (
                        date VARCHAR(10),
                        stock_id VARCHAR(10),
                        foreign_net_buy BIGINT DEFAULT 0,
                        trust_net_buy BIGINT DEFAULT 0,
                        PRIMARY KEY (date, stock_id)
                    );
                """)
            conn.commit()
        print("✅ 成功補上 twse_institutional_investors 資料表！")
    except Exception as e:
        print(f"💥 發生錯誤: {e}")

if __name__ == "__main__":
    fix_missing_table()