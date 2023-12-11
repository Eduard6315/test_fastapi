import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_url = f"sqlite+aiosqlite:///{os.path.join(BASE_DIR, 'db.sqlite3')}"