import sqlite3
import os

# Adjust path if your DB is elsewhere
DB_PATH = os.path.join('app', 'instance', 'labellab.db') 

def add_notifications_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            message TEXT,
            type TEXT DEFAULT 'info',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    print("SUCCESS: Notification table created.")

if __name__ == "__main__":
    add_notifications_table()