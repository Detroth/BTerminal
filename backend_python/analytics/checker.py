import datetime
import aiohttp
import pandas as pd
import asyncio
import aiosqlite
from config import DB_NAME

async def check_signal_outcome(symbol: str, signal_ts_str: str, entry_price: float, tp_price: float, sl_price: float, is_long: bool = False) -> dict:
    clean_symbol = symbol.replace("_", "")
    try:
        dt_obj = datetime.datetime.strptime(signal_ts_str, '%Y-%m-%d %H:%M:%S')
        dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc)
        start_time = int(dt_obj.timestamp() * 1000)
    except ValueError:
        return {"status": "ERROR", "reason": "Date parse fail"}

    url = "https://api.mexc.com/api/v3/klines"
    params = {
        "symbol": clean_symbol,
        "interval": "1m",
        "startTime": start_time,
        "limit": 20
    }
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

    if not klines or not isinstance(klines, list):
        if isinstance(klines, dict) and "msg" in klines:
             return {"status": "API_MSG", "error": klines["msg"]}
        return {"status": "NO_DATA"}

    for i, k in enumerate(klines):
        if i >= 15: break
        if len(k) < 5: continue
        try:
            high = float(k[2])
            low = float(k[3])
        except (ValueError, IndexError):
            continue
        
        if is_long:
            if low < sl_price:
                pnl_pct = ((sl_price - entry_price) / entry_price) * 100
                return {"status": "LOSS", "time_to_take_mins": i + 1, "exit_price": sl_price, "realized_pnl_pct": pnl_pct}
            if high >= tp_price:
                pnl_pct = ((tp_price - entry_price) / entry_price) * 100
                return {"status": "WIN", "time_to_take_mins": i + 1, "exit_price": tp_price, "realized_pnl_pct": pnl_pct}
        else:
            if high > sl_price:
                pnl_pct = ((entry_price - sl_price) / entry_price) * 100
                return {"status": "LOSS", "time_to_take_mins": i + 1, "exit_price": sl_price, "realized_pnl_pct": pnl_pct}
            if low <= tp_price:
                pnl_pct = ((entry_price - tp_price) / entry_price) * 100
                return {"status": "WIN", "time_to_take_mins": i + 1, "exit_price": tp_price, "realized_pnl_pct": pnl_pct}

    last_close = float(klines[min(14, len(klines)-1)][4])
    if is_long: pnl_pct = ((last_close - entry_price) / entry_price) * 100
    else: pnl_pct = ((entry_price - last_close) / entry_price) * 100
    return {"status": "TIME_STOP", "time_to_take_mins": 15, "exit_price": last_close, "realized_pnl_pct": pnl_pct}

async def collect_quant_data():
    async with aiosqlite.connect(DB_NAME) as db:
        query_splash = "SELECT symbol, price, change_pct, volume_24h, timestamp FROM signals_splash WHERE timestamp > datetime('now', '-1 day')"
        async with db.execute(query_splash) as cursor:
            rows_splash = await cursor.fetchall()
            
        query_adv = "SELECT symbol, entry_price, change_pct, volume_24h, timestamp, stop_loss, fair_price FROM signals_advanced WHERE timestamp > datetime('now', '-1 day')"
        async with db.execute(query_adv) as cursor:
            rows_adv = await cursor.fetchall()

        query_fade = "SELECT symbol, entry_price, fair_price, stop_loss, volume_24h, timestamp FROM signals_fade WHERE timestamp > datetime('now', '-1 day')"
        async with db.execute(query_fade) as cursor:
            rows_fade = await cursor.fetchall()

        query_mom = "SELECT symbol, entry_price, fair_price, stop_loss, volume_24h, timestamp FROM signals_momentum WHERE timestamp > datetime('now', '-1 day')"
        async with db.execute(query_mom) as cursor:
            rows_mom = await cursor.fetchall()

    if not rows_splash and not rows_adv and not rows_fade and not rows_mom:
        return None

    results = []

    for row in rows_splash:
        symbol, price, change_pct, vol, ts = row
        implied_low = price / (1 + change_pct / 100.0)
        tp_price = (price + implied_low) / 2.0
        sl_price = price * 1.005
        outcome = await check_signal_outcome(symbol, ts, price, tp_price, sl_price, is_long=False)
        results.append({
            "symbol": symbol, "entry_price": price, "change_pct": change_pct, "volume_24h": vol, "timestamp": ts,
            "type": "SPLASH", "status": outcome.get("status"),
            "time_to_take_mins": outcome.get("time_to_take_mins", 0), "realized_pnl_pct": outcome.get("realized_pnl_pct", 0.0)
        })
        await asyncio.sleep(0.1)

    for row in rows_adv:
        symbol, price, change_pct, vol, ts, sl, fair = row
        outcome = await check_signal_outcome(symbol, ts, price, fair, sl, is_long=False)
        results.append({
            "symbol": symbol, "entry_price": price, "change_pct": change_pct, "volume_24h": vol, "timestamp": ts,
            "type": "ADVANCED", "strategy_tier": "ADVANCED", "status": outcome.get("status"),
            "time_to_take_mins": outcome.get("time_to_take_mins", 0), "realized_pnl_pct": outcome.get("realized_pnl_pct", 0.0)
        })
        await asyncio.sleep(0.1)

    for row in rows_fade:
        symbol, price, fair, sl, vol, ts = row
        outcome = await check_signal_outcome(symbol, ts, price, fair, sl, is_long=False)
        results.append({
            "symbol": symbol, "entry_price": price, "change_pct": 0.0, "volume_24h": vol, "timestamp": ts,
            "type": "FADE", "strategy_tier": "FADE", "status": outcome.get("status"),
            "time_to_take_mins": outcome.get("time_to_take_mins", 0), "realized_pnl_pct": outcome.get("realized_pnl_pct", 0.0)
        })
        await asyncio.sleep(0.1)

    for row in rows_mom:
        symbol, price, fair, sl, vol, ts = row
        outcome = await check_signal_outcome(symbol, ts, price, fair, sl, is_long=True)
        results.append({
            "symbol": symbol, "entry_price": price, "change_pct": 0.0, "volume_24h": vol, "timestamp": ts,
            "type": "MOMENTUM", "strategy_tier": "MOMENTUM", "status": outcome.get("status"),
            "time_to_take_mins": outcome.get("time_to_take_mins", 0), "realized_pnl_pct": outcome.get("realized_pnl_pct", 0.0)
        })
        await asyncio.sleep(0.1)

    df = pd.DataFrame(results)
    if not df.empty:
        mask_splash = df['type'] == 'SPLASH'
        df.loc[mask_splash, 'strategy_tier'] = 'SPLASH_9%'
        df.loc[mask_splash & (df['change_pct'] >= 12), 'strategy_tier'] = 'SPLASH_12%'
        df.loc[mask_splash & (df['change_pct'] >= 50), 'strategy_tier'] = 'SPLASH_50%'
    return df