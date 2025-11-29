from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "keiba.db"

def main():
    print("DB path:", DB_PATH, "exists:", DB_PATH.exists())
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # テーブル一覧
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view');")    
    rows = cur.fetchall()
    cur.execute("SELECT race_id, horse_id, finish_rank, popularity FROM race_results WHERE race_id = 'R2025TOK11' ORDER BY finish_rank")
    for row in cur.fetchall():
        print(row)


    conn.close()

    
    print("objects in DB:")
    for name, t in rows:
        print(f"  - {t}: {name}")

if __name__ == "__main__":
    main()
