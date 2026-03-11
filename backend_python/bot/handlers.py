import asyncio
import io
import datetime
import aiosqlite
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, BufferedInputFile, InputMediaDocument
from bot.loader import bot
from bot.keyboards import get_settings_keyboard
from config import DB_NAME, ADMIN_ID
from engine.shared_state import last_internal_message_time
from analytics.checker import collect_quant_data
from analytics.pdf_generator import generate_quant_charts

router = Router()

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (telegram_id) VALUES (?)", (user_id,))
        await db.commit()
    await message.answer("🚀 <b>SaaS Терминал запущен.</b>\nИспользуйте /settings для настройки персональных фильтров.")

@router.message(Command("settings"))
async def cmd_settings(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (telegram_id) VALUES (?)", (user_id,))
        await db.commit()
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume, fade_enabled, momentum_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
    
    await message.answer("⚙️ <b>Персональные фильтры:</b>\nНастройте уведомления под свой риск-профиль.", reply_markup=get_settings_keyboard(row))

@router.callback_query(F.data == "toggle_splash")
async def cb_toggle_splash(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT splash_threshold FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        if current == 9: new_val = 12
        elif current == 12: new_val = 50
        elif current == 50: new_val = 999
        else: new_val = 9
        
        await db.execute("UPDATE users SET splash_threshold = ? WHERE telegram_id = ?", (new_val, user_id))
        await db.commit()
        
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume, fade_enabled, momentum_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
    
    await callback.message.edit_text("⚙️ <b>Персональные фильтры:</b>\nНастройте уведомления под свой риск-профиль.", reply_markup=get_settings_keyboard(row))
    await callback.answer("Порог обновлен")

@router.callback_query(F.data == "toggle_advanced")
async def cb_toggle_advanced(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT advanced_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        new_val = 0 if current else 1
        await db.execute("UPDATE users SET advanced_enabled = ? WHERE telegram_id = ?", (new_val, user_id))
        await db.commit()
        
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume, fade_enabled, momentum_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            
    await callback.message.edit_text("⚙️ <b>Персональные фильтры:</b>\nНастройте уведомления под свой риск-профиль.", reply_markup=get_settings_keyboard(row))
    await callback.answer("Стратегия переключена")

@router.callback_query(F.data == "toggle_volume")
async def cb_toggle_volume(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT min_volume FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        new_val = 0 if current == 400000 else 400000
        await db.execute("UPDATE users SET min_volume = ? WHERE telegram_id = ?", (new_val, user_id))
        await db.commit()
        
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume, fade_enabled, momentum_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            
    await callback.message.edit_text("⚙️ <b>Персональные фильтры:</b>\nНастройте уведомления под свой риск-профиль.", reply_markup=get_settings_keyboard(row))
    await callback.answer("Фильтр объема обновлен")

@router.callback_query(F.data == "toggle_fade")
async def cb_toggle_fade(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT fade_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        new_val = 0 if current else 1
        await db.execute("UPDATE users SET fade_enabled = ? WHERE telegram_id = ?", (new_val, user_id))
        await db.commit()
        
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume, fade_enabled, momentum_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            
    await callback.message.edit_text("⚙️ <b>Персональные фильтры:</b>\nНастройте уведомления под свой риск-профиль.", reply_markup=get_settings_keyboard(row))
    await callback.answer("Стратегия FADE переключена")

@router.callback_query(F.data == "toggle_momentum")
async def cb_toggle_momentum(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT momentum_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        new_val = 0 if current else 1
        await db.execute("UPDATE users SET momentum_enabled = ? WHERE telegram_id = ?", (new_val, user_id))
        await db.commit()
        
        async with db.execute("SELECT splash_threshold, advanced_enabled, min_volume, fade_enabled, momentum_enabled FROM users WHERE telegram_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            
    await callback.message.edit_text("⚙️ <b>Персональные фильтры:</b>\nНастройте уведомления под свой риск-профиль.", reply_markup=get_settings_keyboard(row))
    await callback.answer("Стратегия MOMENTUM переключена")

@router.message(Command("quant"))
async def cmd_quant(message: types.Message):
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                users_count = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM signals_splash WHERE date(timestamp) = date('now')") as cursor:
                splash_today = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM signals_advanced WHERE date(timestamp) = date('now')") as cursor:
                advanced_today = (await cursor.fetchone())[0]
        
        await message.answer(f"<b>📊 Сводка SaaS-продукта</b>\nАктивных юзеров: {users_count}\nПоймано Сплешей (сегодня): {splash_today}\nПоймано Алгоритмов (сегодня): {advanced_today}")
    except Exception as e:
        await message.answer(f"⚠️ Ошибка получения статистики: {e}")

@router.message(Command("status"))
async def cmd_status(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    # Import shared state inside function to avoid circular import issues if any, though here it's fine
    from engine.shared_state import last_internal_message_time
    if last_internal_message_time:
        time_since_last_signal = datetime.datetime.now() - last_internal_message_time
        status_text = f"✅ <b>Движок активен.</b>\nПоследний сигнал от Rust: {time_since_last_signal.total_seconds():.1f} сек. назад."
    else:
        status_text = "⚠️ <b>Движок неактивен</b> или еще не присылал данных."
    await message.answer(status_text)

@router.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
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
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Failed to broadcast to {uid}: {e}")
            failed_count += 1
    await message.answer(f"✅ Рассылка завершена.\nОтправлено: {sent_count}\nОшибок: {failed_count}")

@router.message(Command("last_alerts"))
async def cmd_last_alerts(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
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

@router.message(Command("list_users"))
async def cmd_list_users(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT telegram_id FROM users") as cursor:
            users = await cursor.fetchall()
    if not users:
        return await message.answer("👥 В системе пока нет пользователей.")
    response_text = f"<b>Список пользователей ({len(users)}):</b>\n"
    for user in users:
        response_text += f"- `{user[0]}`\n"
    await message.answer(response_text)

@router.message(Command("report"))
async def cmd_report(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    status_msg = await message.answer("⏳ Собираю свечи с MEXC и считаю метрики. Это займет пару минут...")
    try:
        df = await collect_quant_data()
        if df is None or df.empty:
            await status_msg.edit_text("🤷‍♂️ За последние 24 часа сигналов не было.")
            return

        chart_buffer = generate_quant_charts(df)
        csv_str = df.to_csv(index=False)
        csv_buffer = io.BytesIO(csv_str.encode('utf-8'))

        summary_lines = ["📊 <b>QUANT REPORT (24h)</b>"]
        if 'strategy_tier' in df.columns:
            tiers = df['strategy_tier'].unique()
            for tier in sorted(tiers):
                tier_df = df[df['strategy_tier'] == tier]
                total = len(tier_df)
                wins = len(tier_df[tier_df["status"] == "WIN"])
                losses = len(tier_df[tier_df["status"] == "LOSS"])
                time_stops = len(tier_df[tier_df["status"] == "TIME_STOP"])
                winrate = (wins / total * 100) if total > 0 else 0.0
                summary_lines.append(f"\n🔹 <b>{tier}</b>")
                summary_lines.append(f"   Sig: {total} | WR: {winrate:.1f}%")
                summary_lines.append(f"   W/L/T: {wins}/{losses}/{time_stops}")
        
        summary_lines.append(f"\nJSON Data for LLM:\n{df.to_json(orient='records')}")
        summary = "\n".join(summary_lines)
        txt_buffer = io.BytesIO(summary.encode('utf-8'))
        
        await status_msg.delete()
        if chart_buffer:
            await message.answer_photo(photo=BufferedInputFile(chart_buffer.getvalue(), filename="quant_charts.png"), caption="📊 <b>Визуализация эффективности</b>")
        await message.answer_media_group([
            InputMediaDocument(media=BufferedInputFile(csv_buffer.getvalue(), filename="daily_quant_data.csv")),
            InputMediaDocument(media=BufferedInputFile(txt_buffer.getvalue(), filename="llm_prompt.txt"), caption="✅ <b>Полный отчет готов</b>")
        ])
    except Exception as e:
        await message.answer(f"⚠️ Ошибка генерации отчета: {e}")