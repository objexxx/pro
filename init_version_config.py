import sqlite3
import os

DB_PATH = os.path.join('app', 'instance', 'labellab.db')

def init_versions():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found. Run the app first.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("--- ⚙️ INITIALIZING VERSION CONFIG ---")
    
    versions = ['95055', '94888', '94019', '95888', '91149', '93055']
    
    for v in versions:
        # Default: Enabled (1), Price (3.00)
        c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", (f"ver_en_{v}", "1"))
        c.execute("INSERT OR IGNORE INTO system_config (key, value) VALUES (?, ?)", (f"ver_price_{v}", "3.00"))
        print(f"✅ Version {v} initialized.")

    conn.commit()
    conn.close()
    print("\n✨ Version Control System Ready.")

if __name__ == "__main__":
    init_versions()