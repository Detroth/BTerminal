import datetime
import asyncio
import aiosqlite
from typing import List
from fastapi import WebSocket, WebSocketDisconnect
from config import DB_NAME
from bot.loader import bot
import engine.shared_state as shared_state

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

async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

async def internal_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            shared_state.last_internal_message_time = datetime.datetime.now()
            data = await websocket.receive_json()
            await manager.broadcast(data)
            
            if data.get("type") == "momentum":
                items = data.get("data", [])
                if not isinstance(items, list): items = [items]
                
                async with aiosqlite.connect(DB_NAME) as db:
                    for item in items:
                        signal_type = item.get("signal_type")
                        symbol = item.get("symbol")
                        price = item.get("current_price")
                        fair_price = item.get("fair_price")
                        change_pct = item.get("change_pct")
                        stop_loss = item.get("stop_loss", 0.0)
                        volume_24h = item.get("volume_24h")
                        ts_raw = item.get("timestamp", 0)
                        
                        print(f"🔍 ROUTING: {symbol} {signal_type} +{change_pct:.2f}% Vol: {volume_24h:,.0f}")
                        timestamp_converted = datetime.datetime.fromtimestamp(ts_raw).strftime('%Y-%m-%d %H:%M:%S')
                        
                        # Форматируем символ для отображения (убираем _USDT)
                        display_symbol = symbol.split('_')[0]

                        if signal_type == "SPLASH":
                            await db.execute("INSERT INTO signals_splash (symbol, price, change_pct, volume_24h, timestamp) VALUES (?, ?, ?, ?, ?)", (symbol, price, change_pct, volume_24h, timestamp_converted))
                        elif signal_type == "ADVANCED":
                            await db.execute("INSERT INTO signals_advanced (symbol, entry_price, fair_price, change_pct, volume_24h, timestamp, stop_loss) VALUES (?, ?, ?, ?, ?, ?, ?)", (symbol, price, fair_price, change_pct, volume_24h, timestamp_converted, stop_loss))
                        elif signal_type == "FADE":
                            await db.execute("INSERT INTO signals_fade (symbol, entry_price, fair_price, stop_loss, volume_24h, timestamp) VALUES (?, ?, ?, ?, ?, ?)", (symbol, price, fair_price, stop_loss, volume_24h, timestamp_converted))
                        elif signal_type == "MOMENTUM":
                            await db.execute("INSERT INTO signals_momentum (symbol, entry_price, fair_price, stop_loss, volume_24h, timestamp) VALUES (?, ?, ?, ?, ?, ?)", (symbol, price, fair_price, stop_loss, volume_24h, timestamp_converted))
                        await db.commit()

                        async with db.execute("SELECT telegram_id, splash_threshold, advanced_enabled, min_volume, fade_enabled, momentum_enabled FROM users") as cursor:
                            users = await cursor.fetchall()
                        
                        for user in users:
                            uid, splash_th, adv_en, min_vol, fade_en, mom_en = user
                            if volume_24h < min_vol: continue
                            
                            send = False
                            if signal_type == "SPLASH":
                                if change_pct >= splash_th: send = True
                            elif signal_type == "ADVANCED":
                                if adv_en: send = True
                            elif signal_type == "FADE":
                                if fade_en: send = True
                            elif signal_type == "MOMENTUM":
                                if mom_en: send = True
                            
                            if send:
                                if signal_type == "FADE":
                                    msg_text = f"<b>📉 Затухание Объема (FADE)</b>\n\nПара: <code>{display_symbol}</code>\n\nВход: <code>{price}</code>\nТейк: <code>{fair_price}</code>\nСтоп (ATR): <code>{stop_loss}</code>\n\nОбъем 24h: ${volume_24h:,.0f}\nВремя: {timestamp_converted}"
                                elif signal_type == "MOMENTUM":
                                    msg_text = f"<b>🚀 Импульсный Пробой (MOMENTUM)</b>\n\nПара: <code>{display_symbol}</code>\n\nЛонг от: <code>{price}</code>\nТейк: <code>{fair_price}</code>\nСтоп: <code>{stop_loss}</code>\n\nОбъем 24h: ${volume_24h:,.0f}\nВремя: {timestamp_converted}"
                                else:
                                    msg_text = f"<b>⚡️ {signal_type}</b>\n\nПара: <code>{display_symbol}</code>\nИзменение: +{change_pct:.2f}%\n\nЦена: {price}\nСправедливая цена: {fair_price}\n\nОбъем 24h: ${volume_24h:,.0f}\nВремя: {timestamp_converted}"
                                
                                try:
                                    await bot.send_message(uid, msg_text)
                                except Exception as e:
                                    print(f"Failed to send to {uid}: {e}")
    except Exception as e:
        print(f"Internal engine disconnected: {e}")