import sqlite3
from pathlib import Path

def setup_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ids (
            id TEXT PRIMARY KEY
        )
    ''')
    conn.commit()
    conn.close()


def count_records():
    conn = sqlite3.connect("generated_ids.db")
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) FROM ids
    ''')
    count = cursor.fetchone()[0]
    conn.close()
    return count


def count_ids():
    conn = sqlite3.connect("generated_ids.db")
    cursor = conn.cursor()
    cursor.execute('''
        SELECT distinct COUNT(*) FROM ids
    ''')
    count = cursor.fetchone()[0]
    conn.close()
    return count


def truncate_table():
    conn = sqlite3.connect("generated_ids.db")
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM ids
    ''')
    conn.commit()
    conn.close()


if __name__ == "__main__":
    DB_FILE = Path('./generated_ids.db')
    setup_database()

    print("Total records in the database    :", count_records())
    print("Total unique IDs in the database :", count_ids())

