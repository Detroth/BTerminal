import asyncio
import os
import datetime
from typing import List, Set
import aiohttp
import pandas as pd
import io
from contextlib import asynccontextmanager
import matplotlib
matplotlib.use('Agg') # Устанавливаем неинтерактивный бэкенд для сервера
import matplotlib.pyplot as plt
import seaborn as sns
import aiosqlite
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# --- TELEGRAM BOT SETUP ---
#API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_TOKEN = "8634666749:AAGtDEll_hLpQm2VKfFZHbCNPUSW8KRPcNg"
ADMIN_ID = int(os.getenv("ADMIN_ID", "1115714808")) # ID админа для доступа к статистике

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- GLOBAL STATE ---
last_internal_message_time = None

# --- DATABASE SETUP ---
DB_NAME = os.environ.get("DB_PATH", "quant_journal.db")

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Таблица пользователей и их настроек
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                splash_threshold INTEGER DEFAULT 9,
                advanced_enabled BOOLEAN DEFAULT 1,
                min_volume REAL DEFAULT 400000
            )
        """)
        # Таблица сигналов SPLASH
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
        # Таблица сигналов ADVANCED
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
        await db.commit()

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (telegram_id) VALUES (?)", (user_id,))
        await db.commit()
    await message.answer("🚀 <b>SaaS Терминал запущен.</b>\nИспользуйте /settings для настройки персональных фильтров.")

# --- USER SETTINGS & UI ---

@dp.message(Command("settings"))
async def cmd_settings(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        # Регистрируем если нет, или получаем настройки
        await db.execute("INSERT OR IGNORE INTO users (telegram_id) VALUES (?)", (user_id,))
        await db.commit()
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
    
    await send_settings_keyboard(message, row)

async def send_settings_keyboard(message_or_callback, settings):
    splash_th, adv_en, min_vol = settings
    
    # Текст кнопок
    splash_text = f"🌊 Сплеши: >{splash_th}%" if splash_th < 100 else "🌊 Сплеши: ВЫКЛ"
    adv_text = f"🧠 Advanced: {'ВКЛ' if adv_en else 'ВЫКЛ'}"
    vol_text = f"📊 Объем: >${int(min_vol/1000)}k" if min_vol > 0 else "📊 Объем: Любой"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=splash_text, callback_data="toggle_splash")],
        [InlineKeyboardButton(text=adv_text, callback_data="toggle_advanced")],
        [InlineKeyboardButton(text=vol_text, callback_data="toggle_volume")]
    ])
    
    text = "⚙️ <b>Персональные фильтры:</b>\nНастройте уведомления под свой риск-профиль."
    
    if isinstance(message_or_callback, types.Message):
        await message_or_callback.answer(text, reply_markup=keyboard)
    elif isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(text, reply_markup=keyboard)

@dp.callback_query(F.data == "toggle_splash")
async def cb_toggle_splash(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT splash_threshold FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        # Логика переключения: 9 -> 12 -> 50 -> 999 (Выкл) -> 9
        if current == 9: new_val = 12
        elif current == 12: new_val = 50
        elif current == 50: new_val = 999
        else: new_val = 9
        
        await db.execute("UPDATE users SET splash_threshold = ? WHERE telegram_id = ?", (new_val, user_id))
        await db.commit()
        
        # Обновляем UI
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
    
    await send_settings_keyboard(callback, row)
    await callback.answer("Порог обновлен")

@dp.callback_query(F.data == "toggle_advanced")
async def cb_toggle_advanced(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT advanced_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        new_val = 0 if current else 1
        await db.execute("UPDATE users SET advanced_enabled = ? WHERE telegram_id = ?", (new_val, user_id))
        await db.commit()
        
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            
    await send_settings_keyboard(callback, row)
    await callback.answer("Стратегия переключена")

@dp.callback_query(F.data == "toggle_volume")
async def cb_toggle_volume(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT min_volume FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        new_val = 0 if current == 400000 else 400000
        await db.execute("UPDATE users SET min_volume = ? WHERE telegram_id = ?", (new_val, user_id))
        await db.commit()
        
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            
    await send_settings_keyboard(callback, row)
    await callback.answer("Фильтр объема обновлен")

@dp.message(Command("quant"))
async def cmd_quant(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return # Игнорируем не-админов
        
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                users_count = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM signals_splash WHERE date(timestamp) = date('now')") as cursor:
                splash_today = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM signals_advanced WHERE date(timestamp) = date('now')") as cursor:
                advanced_today = (await cursor.fetchone())[0]
        
        await message.answer(
            f"<b>📊 Сводка SaaS-продукта</b>\n"
            f"Активных юзеров: {users_count}\n"
            f"Поймано Сплешей (сегодня): {splash_today}\n"
            f"Поймано Алгоритмов (сегодня): {advanced_today}"
        )
    except Exception as e:
        await message.answer(f"⚠️ Ошибка получения статистики: {e}")

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    global last_internal_message_time
    if last_internal_message_time:
        time_since_last_signal = datetime.datetime.now() - last_internal_message_time
        status_text = f"✅ <b>Движок активен.</b>\nПоследний сигнал от Rust: {time_since_last_signal.total_seconds():.1f} сек. назад."
    else:
        status_text = "⚠️ <b>Движок неактивен</b> или еще не присылал данных."
    
    await message.answer(status_text)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    text_to_send = message.text.replace("/broadcast", "").strip()
    if not text_to_send:
        await message.answer("⚠️ Укажите текст для рассылки после команды.\nПример: `/broadcast Всем привет!`")
        return

    sent_count = 0
    failed_count = 0
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT telegram_id FROM users") as cursor:
            users = await cursor.fetchall()
    
    await message.answer(f"🚀 Начинаю рассылку для {len(users)} пользователей...")

    for user in users:
        uid = user[0]
        try:
            await bot.send_message(uid, text_to_send)
            sent_count += 1
            await asyncio.sleep(0.1) # Защита от rate-limit со стороны Telegram
        except Exception as e:
            print(f"Failed to broadcast to {uid}: {e}")
            failed_count += 1
    
    await message.answer(f"✅ Рассылка завершена.\nОтправлено: {sent_count}\nОшибок: {failed_count}")

@dp.message(Command("last_alerts"))
async def cmd_last_alerts(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    async with aiosqlite.connect(DB_NAME) as db:
        query = """
        SELECT symbol, 'SPLASH' as signal_type, change_pct, timestamp FROM signals_splash
        UNION ALL
        SELECT symbol, 'ADVANCED' as signal_type, change_pct, timestamp FROM signals_advanced
        ORDER BY timestamp DESC
        LIMIT 3
        """
        async with db.execute(query) as cursor:
            last_signals = await cursor.fetchall()

    if not last_signals:
        return await message.answer("🤷‍♂️ Еще не было отправлено ни одного алерта.")

    response_text = "<b>Последние 3 отправленных сигнала:</b>\n\n"
    for symbol, signal_type, change_pct, ts in last_signals:
        response_text += f"<b>{signal_type}</b>: #{symbol} (+{change_pct:.2f}%) в {ts}\n"
    
    await message.answer(response_text)

@dp.message(Command("list_users"))
async def cmd_list_users(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT telegram_id FROM users") as cursor:
            users = await cursor.fetchall()
    
    if not users:
        return await message.answer("👥 В системе пока нет пользователей.")
    
    response_text = f"<b>Список пользователей ({len(users)}):</b>\n"
    for user in users:
        response_text += f"- `{user[0]}`\n"
    
    await message.answer(response_text)

# --- QUANT CHECKER & REPORTING ---

async def check_signal_outcome(symbol: str, signal_ts_str: str, entry_price: float, change_pct: float) -> dict:
    """
    Валидирует сигнал по историческим данным MEXC (Spot API).
    Стратегия: Short (Mean Reversion) на пампе.
    """
    # 1. Подготовка параметров
    # MEXC Spot V3 требует формат BTCUSDT, а у нас BTC_USDT
    clean_symbol = symbol.replace("_", "")
    
    try:
        dt_obj = datetime.datetime.strptime(signal_ts_str, '%Y-%m-%d %H:%M:%S')
        # Принудительно ставим UTC, чтобы timestamp() не использовал локальное время сервера
        dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc)
        start_time = int(dt_obj.timestamp() * 1000)
    except ValueError:
        return {"status": "ERROR", "reason": "Date parse fail"}

    # 2. Расчет уровней (предполагаем Short на пампе)
    # Если цена выросла на change_pct, значит Low движения был:
    implied_low = entry_price / (1 + change_pct / 100.0)
    
    # Take Profit: 50% отката движения (Fair Price)
    tp_price = (entry_price + implied_low) / 2.0
    
    # Stop Loss: Хай сплеша (в данном случае Entry Price, так как мы ловим вершину)
    # Даем небольшой буфер 0.5% вверх, чтобы не выбивало шумом
    sl_price = entry_price * 1.005

    # 3. Запрос к API
    url = "https://api.mexc.com/api/v3/klines"
    params = {
        "symbol": clean_symbol,
        "interval": "1m",
        "startTime": start_time,
        "limit": 120 # 2 часа на отработку
    }

    # Добавляем User-Agent, так как MEXC часто блокирует запросы без него (Cloudflare)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status != 200:
                    return {"status": "API_ERROR", "code": resp.status}
                klines = await resp.json()
        except Exception as e:
            return {"status": "NET_ERROR", "error": str(e)}

    # Учитываем опыт: проверяем, что пришел именно список (данные), а не словарь (ошибка)
    if not klines or not isinstance(klines, list):
        if isinstance(klines, dict) and "msg" in klines:
             return {"status": "API_MSG", "error": klines["msg"]}
        return {"status": "NO_DATA"}

    # 4. Анализ свечей
    # kline format: [time, open, high, low, close, vol, ...]
    for i, k in enumerate(klines):
        high = float(k[2])
        low = float(k[3])
        # Защита от битых данных
        if len(k) < 5: continue
        
        try:
            # Цены приходят строками, конвертируем
            high = float(k[2])
            low = float(k[3])
        except (ValueError, IndexError):
            continue
        
        # Проверка SL (цена ушла выше хая)
        if high > sl_price:
            return {"status": "LOSS", "time_to_take_mins": i + 1, "exit_price": high}
        
        # Проверка TP (цена коснулась справедливой цены)
        if low <= tp_price:
            return {"status": "WIN", "time_to_take_mins": i + 1, "exit_price": tp_price}

    return {"status": "TIMEOUT", "time_to_take_mins": 120}

def generate_quant_charts(df: pd.DataFrame):
    if df is None or df.empty:
        return None
    
    # Подготовка данных
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
    
    # Фильтруем только завершенные сделки
    df_clean = df[df['status'].isin(['WIN', 'LOSS'])].copy()
    if df_clean.empty:
        return None

    sns.set_theme(style="darkgrid")
    fig, axes = plt.subplots(3, 1, figsize=(10, 15))
    plt.subplots_adjust(hspace=0.4)

    # График 1: Активность и Винрейт по часам
    hourly_stats = df_clean.groupby('hour').agg(
        count=('status', 'count'),
        wins=('status', lambda x: (x == 'WIN').sum())
    ).reset_index()
    hourly_stats['winrate'] = (hourly_stats['wins'] / hourly_stats['count']) * 100
    
    # Заполняем пропуски часов (чтобы ось X была полной 0-23)
    all_hours = pd.DataFrame({'hour': range(24)})
    hourly_stats = pd.merge(all_hours, hourly_stats, on='hour', how='left').fillna(0)

    ax1 = axes[0]
    sns.barplot(data=hourly_stats, x='hour', y='count', ax=ax1, color='steelblue', alpha=0.7)
    ax1.set_ylabel('Signals Count')
    ax1.set_title('Распределение сигналов и Winrate по часам (UTC)')
    
    ax2 = ax1.twinx()
    sns.lineplot(data=hourly_stats, x='hour', y='winrate', ax=ax2, color='red', marker='o')
    ax2.set_ylabel('Winrate (%)')
    ax2.set_ylim(0, 105)

    # График 2: Объем vs Время
    ax3 = axes[1]
    if 'volume_24h' in df_clean.columns and 'time_to_take_mins' in df_clean.columns:
        sns.scatterplot(data=df_clean, x='volume_24h', y='time_to_take_mins', hue='status', 
                        palette={'WIN': 'green', 'LOSS': 'red'}, ax=ax3, alpha=0.6)
        ax3.set_xscale('log')
        ax3.set_title('Объем 24h vs Время до Тейк-Профита')

    # График 3: Equity
    ax4 = axes[2]
    df_sorted = df_clean.sort_values('timestamp')
    df_sorted['pnl'] = df_sorted['status'].apply(lambda x: 1 if x == 'WIN' else -1)
    df_sorted['equity'] = df_sorted['pnl'].cumsum()
    
    sns.lineplot(data=df_sorted, x=range(len(df_sorted)), y='equity', ax=ax4, color='purple')
    ax4.fill_between(range(len(df_sorted)), df_sorted['equity'], alpha=0.3, color='purple')
    ax4.set_title('Динамика эффективности (Win/Loss Equity)')

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)
    return buf

async def collect_quant_data():
    """Собирает данные по сигналам за 24 часа и проверяет их исход."""
    async with aiosqlite.connect(DB_NAME) as db:
        query = "SELECT symbol, price, change_pct, volume_24h, timestamp FROM signals_splash WHERE timestamp > datetime('now', '-1 day')"
        async with db.execute(query) as cursor:
            rows = await cursor.fetchall()

    if not rows:
        return None

    results = []
    for row in rows:
        symbol, price, change_pct, vol, ts = row
        outcome = await check_signal_outcome(symbol, ts, price, change_pct)
        
        results.append({
            "symbol": symbol,
            "entry_price": price,
            "change_pct": change_pct,
            "volume_24h": vol,
            "timestamp": ts,
            "status": outcome.get("status"),
            "time_to_take_mins": outcome.get("time_to_take_mins", 0)
        })
        await asyncio.sleep(0.1)

    return pd.DataFrame(results)

@dp.message(Command("report"))
async def cmd_report(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    status_msg = await message.answer("⏳ Собираю свечи с MEXC и считаю метрики. Это займет пару минут...")
    
    try:
        # 1. Сбор данных
        df = await collect_quant_data()
        
        if df is None or df.empty:
            await status_msg.edit_text("🤷‍♂️ За последние 24 часа сигналов не было.")
            return

        # 2. Генерация графиков
        chart_buffer = generate_quant_charts(df)

        # 3. Подготовка файлов (CSV и TXT)
        # CSV (через строку, т.к. BytesIO требует байты, а to_csv пишет str)
        csv_str = df.to_csv(index=False)
        csv_buffer = io.BytesIO(csv_str.encode('utf-8'))

        # Текстовая сводка
        total = len(df)
        wins = len(df[df["status"] == "WIN"])
        winrate = (wins / total * 100) if total > 0 else 0
        win_df = df[df["status"] == "WIN"]
        avg_ttt = win_df["time_to_take_mins"].mean() if not win_df.empty else 0

        summary = f"""📊 QUANT REPORT (24h)
Total Signals: {total}
Winrate: {winrate:.2f}%
Avg Time to Take: {avg_ttt:.1f} min

JSON Data for LLM:
{df.to_json(orient="records")}
"""
        txt_buffer = io.BytesIO(summary.encode('utf-8'))
        
        # 4. Отправка сообщений
        await status_msg.delete()

        # Сообщение 1: Графики
        if chart_buffer:
            await message.answer_photo(
                photo=types.BufferedInputFile(chart_buffer.getvalue(), filename="quant_charts.png"),
                caption="📊 <b>Визуализация эффективности</b>"
            )

        # Сообщение 2: Файлы данных
        await message.answer_media_group([
            types.InputMediaDocument(media=types.BufferedInputFile(csv_buffer.getvalue(), filename="daily_quant_data.csv")),
            types.InputMediaDocument(media=types.BufferedInputFile(txt_buffer.getvalue(), filename="llm_prompt.txt"), caption="✅ <b>Полный отчет готов</b>")
        ])
        
    except Exception as e:
        await message.answer(f"⚠️ Ошибка генерации отчета: {e}")

async def keep_alive_ping():
    # FIX: Используем URL из окружения Render, или заглушку для локального теста
    EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:10000")
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

# Внутренний WebSocket для Rust-движка
@app.websocket("/ws/internal")
async def internal_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            global last_internal_message_time
            last_internal_message_time = datetime.datetime.now()

            data = await websocket.receive_json()
            await manager.broadcast(data)
            
            # Роутер сигналов
            if data.get("type") == "momentum":
                items = data.get("data", [])
                if not isinstance(items, list):
                    items = [items]
                
                # print(f"📩 [INTERNAL] Получен сигнал Momentum: {len(items)} шт.")

                async with aiosqlite.connect(DB_NAME) as db:
                    for item in items:
                        signal_type = item.get("signal_type")
                        symbol = item.get("symbol")
                        price = item.get("current_price")
                        fair_price = item.get("fair_price")
                        change_pct = item.get("change_pct")
                        volume_24h = item.get("volume_24h")
                        ts_raw = item.get("timestamp", 0)
                        
                        print(f"🔍 ROUTING: {symbol} {signal_type} +{change_pct:.2f}% Vol: {volume_24h:,.0f}")

                        # Конвертация времени
                        timestamp_converted = datetime.datetime.fromtimestamp(ts_raw).strftime('%Y-%m-%d %H:%M:%S')

                        # 1. Сохранение в БД
                        if signal_type == "SPLASH":
                            await db.execute("INSERT INTO signals_splash (symbol, price, change_pct, volume_24h, timestamp) VALUES (?, ?, ?, ?, ?)", (symbol, price, change_pct, volume_24h, timestamp_converted))
                        elif signal_type == "ADVANCED":
                            await db.execute("INSERT INTO signals_advanced (symbol, entry_price, fair_price, change_pct, volume_24h, timestamp) VALUES (?, ?, ?, ?, ?, ?)", (symbol, price, fair_price, change_pct, volume_24h, timestamp_converted))
                        await db.commit()

                        # 2. Рассылка пользователям (Роутинг)
                        async with db.execute("SELECT telegram_id, splash_threshold, advanced_enabled, min_volume FROM users") as cursor:
                            users = await cursor.fetchall()
                        
                        for user in users:
                            uid, splash_th, adv_en, min_vol = user
                            
                            # Фильтр объема
                            if volume_24h < min_vol:
                                print(f"  ❌ User {uid}: Skip Low Vol ({volume_24h:,.0f} < {min_vol:,.0f})")
                                continue
                            
                            send = False
                            if signal_type == "SPLASH":
                                if change_pct >= splash_th: 
                                    send = True
                                else:
                                    print(f"  ❌ User {uid}: Skip Low Pct ({change_pct:.2f}% < {splash_th}%)")
                            elif signal_type == "ADVANCED":
                                if adv_en: 
                                    send = True
                                else:
                                    print(f"  ❌ User {uid}: Skip Advanced Disabled")
                            
                            if send:
                                msg_text = f"<b>⚡️ {signal_type}</b>\nПара: #{symbol}\nИзменение: +{change_pct:.2f}%\nЦена: {price}\nСправедливая цена: {fair_price}\nОбъем 24h: ${volume_24h:,.0f}\nВремя: {timestamp_converted}"
                                try:
                                    print(f"  ✅ User {uid}: SENDING ALERT...")
                                    await bot.send_message(uid, msg_text)
                                except Exception as e:
                                    print(f"Failed to send to {uid}: {e}")

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
