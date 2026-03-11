import aiosqlite
from config import DB_NAME

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Users table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                splash_threshold INTEGER DEFAULT 9,
                advanced_enabled BOOLEAN DEFAULT 1,
                min_volume REAL DEFAULT 400000
            )
        """)
        # Migrations for new settings
        try:
            await db.execute("ALTER TABLE users ADD COLUMN fade_enabled BOOLEAN DEFAULT 1")
        except Exception:
            pass # Column already exists
        
        try:
            await db.execute("ALTER TABLE users ADD COLUMN momentum_enabled BOOLEAN DEFAULT 1")
        except Exception:
            pass # Column already exists

        # Signals tables
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signals_splash (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                price REAL,
                change_pct REAL,
                volume_24h REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signals_advanced (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                entry_price REAL,
                fair_price REAL,
                change_pct REAL,
                volume_24h REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        try:
            await db.execute("ALTER TABLE signals_advanced ADD COLUMN stop_loss REAL")
        except Exception:
            pass

        await db.execute("""
            CREATE TABLE IF NOT EXISTS signals_fade (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                entry_price REAL,
                fair_price REAL,
                stop_loss REAL,
                volume_24h REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signals_momentum (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                entry_price REAL,
                fair_price REAL,
                stop_loss REAL,
                volume_24h REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()