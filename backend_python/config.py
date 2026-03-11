import os

#API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8634666749:AAFItUC-9ObBlSEWBJGpTmgyJwn1T2UVXUk")
API_TOKEN = "8634666749:AAHqJEMmYa3cqYm24Fy52HDhNpjBvyAgmRg"
ADMIN_ID = int(os.getenv("ADMIN_ID", "1115714808"))
DB_NAME = os.environ.get("DB_PATH", "quant_journal.db")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:10000")