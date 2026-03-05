import asyncio
import os
from typing import List, Set
import aiohttp
from contextlib import asynccontextmanager
import aiosqlite
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# --- TELEGRAM BOT SETUP ---
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Хранилище подписчиков (в памяти)
subscribed_users: Set[int] = set()

# --- DATABASE SETUP ---
DB_NAME = os.environ.get("DB_PATH", "quant_journal.db")

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signals_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                symbol TEXT,
                signal_type TEXT,
                entry_price REAL,
                take_profit REAL,
                stop_loss REAL,
                status TEXT DEFAULT 'OPEN'
            )
        """)
        await db.commit()

async def save_signal_to_db(symbol: str, signal_type: str, entry: float, tp: float, sl: float):
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO signals_history (symbol, signal_type, entry_price, take_profit, stop_loss) VALUES (?, ?, ?, ?, ?)",
                (symbol, signal_type, entry, tp, sl)
            )
            await db.commit()
            print(f"💾 [DB] Saved signal: {symbol} {signal_type}")
    except Exception as e:
        print(f"⚠️ [DB] Error saving signal: {e}")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    subscribed_users.add(message.chat.id)
    print(f"✅ [BOT] Новый подписчик: {message.chat.id}")
    await message.answer("🚀 <b>Терминал запущен.</b>\nОжидаю сигналы с рынка...")

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT COUNT(*) FROM signals_history WHERE date(timestamp) = date('now')") as cursor:
                today_signals_count = (await cursor.fetchone())[0]
        
        await message.answer(
            f"<b>🟢 Статус Движка</b>\n"
            f"Соединение с MEXC: Активно\n"
            f"Сигналов за сегодня: {today_signals_count}\n"
            f"Радар работает в штатном режиме."
        )
    except Exception as e:
        await message.answer(f"⚠️ Ошибка получения статуса: {e}")

@dp.message(Command("quant"))
async def cmd_quant(message: types.Message):
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT COUNT(*) FROM signals_history WHERE status = 'OPEN'") as cursor:
                open_trades = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM signals_history WHERE status = 'WIN'") as cursor:
                win_trades = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM signals_history WHERE status = 'LOSS'") as cursor:
                loss_trades = (await cursor.fetchone())[0]
        
        await message.answer(
            f"<b>🗄 Quant-Журнал (Paper Trading)</b>\n"
            f"Открытых сделок (Ожидают TP/SL): {open_trades}\n"
            f"Закрытых в плюс (WIN): {win_trades}\n"
            f"Закрытых в минус (LOSS): {loss_trades}\n"
            f"──────────────\n"
            f"Сбор датасета для модели ожиданий продолжается."
        )
    except Exception as e:
        await message.answer(f"⚠️ Ошибка получения статистики: {e}")

async def keep_alive_ping():
    EXTERNAL_URL = "https://quant-test.onrender.com"
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(EXTERNAL_URL) as response:
                    pass
        except Exception as e:
            print(f"⚠️ [PING] Error: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: запускаем поллинг бота в фоне
    await init_db()
    polling_task = asyncio.create_task(dp.start_polling(bot))
    ping_task = asyncio.create_task(keep_alive_ping())
    yield
    # Shutdown: останавливаем поллинг
    polling_task.cancel()
    ping_task.cancel()
    try:
        await polling_task
        await ping_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

# Менеджер соединений для управления клиентами
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

# Публичный WebSocket для фронтенда
@app.websocket("/ws/market_data")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Держим соединение открытым
    except WebSocketDisconnect:
        manager.disconnect(websocket)

async def send_telegram_alert(text: str):
    if not subscribed_users:
        print("⚠️ [BOT] Нет подписчиков для отправки уведомления! (Отправьте /start)")
        return

    for chat_id in subscribed_users:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            print(f"Failed to send TG message: {e}")

# Внутренний WebSocket для Rust-движка
@app.websocket("/ws/internal")
async def internal_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_json()
            await manager.broadcast(data)
            
            # Обработка сигналов для Telegram
            if data.get("type") == "momentum":
                items = data.get("data", [])
                if not isinstance(items, list):
                    items = [items]
                
                # Лог для отладки: видим, что сигнал пришел
                print(f"📩 [INTERNAL] Получен сигнал Momentum: {len(items)} шт.")

                for item in items:
                    signal = item.get("signal")
                    symbol = item.get("symbol")
                    change = item.get("change_pct", 0)
                    
                    text = ""
                    if signal == "MACRO_PUMP":
                        text = f"<b>🟢 ЗАРОЖДЕНИЕ ТРЕНДА (15m)</b>\nПара: #{symbol}\nДетали: Аномальный рост объемов (x10) и цены. Крупный игрок набирает позицию."
                        # Save to DB (no TP/SL)
                        price = item.get("price", 0)
                        asyncio.create_task(save_signal_to_db(symbol, signal, price, 0, 0))
                    elif signal == "EXHAUSTION":
                        text = f"<b>🔴 ИСТОЩЕНИЕ ПОКУПАТЕЛЯ (3m)</b>\nПара: #{symbol}\nДетали: Огромный объем сдерживается лимитными ордерами. Цена остановилась. Приготовиться к контр-сделке (Short) или закрытию лонга!"
                        # Save to DB (no TP/SL)
                        price = item.get("price", 0)
                        asyncio.create_task(save_signal_to_db(symbol, signal, price, 0, 0))
                    elif signal == "ALGO_REVERSION":
                        price = item.get("price", 0)
                        high = item.get("high", 0)
                        tp = item.get("tp", 0)
                        sl = item.get("sl", 0)
                        text = f"<b>🤖 АЛГОРИТМИЧЕСКИЙ ОТКАТ (ШОРТ)</b>\nПара: #{symbol}\nТекущая цена: {price:.5f}\n──────────────\n🔴 <b>Stop Loss:</b> {sl:.5f} (за хай {high:.5f})\n🟢 <b>Take Profit:</b> {tp:.5f} (50% коррекции)\n──────────────\nЛогика: Органический рост завершен, алгоритмы возвращают цену в зону баланса. Заходим со стопом!"
                        
                        # Save to DB
                        asyncio.create_task(save_signal_to_db(symbol, signal, price, tp, sl))
                    
                    if text:
                        asyncio.create_task(send_telegram_alert(text))
    except Exception as e:
        print(f"Internal engine disconnected: {e}")

# Подключение статики
# Вычисляем абсолютный путь к папке frontend относительно текущего файла
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "../frontend")

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# Заглушка для Chrome DevTools, чтобы не было ошибки 404 в консоли
@app.get("/.well-known/appspecific/com.chrome.devtools.json", include_in_schema=False)
async def chrome_devtools():
    return Response(status_code=204)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@app.get("/")
async def read_root():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))
