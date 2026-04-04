import os
import sys
import pandas as pd
from database import get_connection


def import_from_df(df: pd.DataFrame) -> int:
    """將 DataFrame 寫入 stock_category，回傳寫入筆數。
    DataFrame 需包含欄位：代號、公司名稱、新產業類別
    """
    def _clean_id(val):
        try:
            return None if pd.isna(val) else str(int(float(val)))
        except Exception:
            return None

    df_clean = pd.DataFrame({
        'stock_id':      df['代號'].apply(_clean_id),
        'company_name':  df['公司名稱'],
        'category_name': df['新產業類別'],
    }).dropna(subset=['stock_id'])

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_category (
                    stock_id      VARCHAR(10) PRIMARY KEY,
                    category_name VARCHAR(50),
                    company_name  VARCHAR(100)
                );
            """)
            cur.executemany("""
                INSERT INTO stock_category (stock_id, company_name, category_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (stock_id) DO UPDATE SET
                    category_name = EXCLUDED.category_name,
                    company_name  = EXCLUDED.company_name;
            """, [(r['stock_id'], r['company_name'], r['category_name'])
                  for _, r in df_clean.iterrows()])
            conn.commit()

    return len(df_clean)


def import_from_file(file_path: str) -> int:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"找不到檔案: {file_path}")
    print(f"📖 讀取: {file_path}")
    return import_from_df(pd.read_csv(file_path))


if __name__ == "__main__":
    path = (sys.argv[1] if len(sys.argv) > 1
            else os.path.join(os.path.dirname(__file__), "..", "960803-0960203558-2.csv"))
    count = import_from_file(path)
    print(f"🎉 匯入完成：{count} 筆")
