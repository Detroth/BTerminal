use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

// Структура для парсинга отдельного тикера из массива Binance
// Поля сокращены: s = symbol, c = close price
#[derive(Debug, Deserialize, Serialize, Clone)]
struct TickerEvent {
    s: String,
    c: String,
    #[serde(rename = "P")]
    p: String,
    #[serde(rename = "q")]
    q: String,
}

// Структуры для Funding Rate API
#[derive(Deserialize)]
struct BinanceFunding {
    symbol: String,
    #[serde(rename = "lastFundingRate")]
    last_funding_rate: String,
}

#[derive(Deserialize)]
struct BybitResponse {
    result: BybitResult,
}

#[derive(Deserialize)]
struct BybitResult {
    list: Vec<BybitTicker>,
}

#[derive(Deserialize)]
struct BybitTicker {
    symbol: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
}

#[derive(Deserialize)]
struct OkxResponse {
    data: Vec<OkxTicker>,
}

#[derive(Deserialize)]
struct OkxTicker {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
}

struct TickData {
    price: f64,
    volume: f64,
    raw_q: f64,
    timestamp: u64,
}

struct ScannerState {
    history: HashMap<String, VecDeque<TickData>>,
    cooldowns: HashMap<String, Instant>,
}

#[tokio::main]
async fn main() {
    // Публичный стрим Binance для всех тикеров (обновляется раз в секунду)
    let connect_addr = "wss://fstream.binance.com/ws/!ticker@arr";
    let url = Url::parse(connect_addr).expect("Bad URL");

    println!("Connecting to Binance WebSocket: {}", connect_addr);

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected via WebSocket");

    // Создаем канал: много отправителей (Tasks) -> один получатель (WebSocket Writer)
    let (tx, mut rx) = mpsc::unbounded_channel();

    // Задача 1: WebSocket Writer с логикой реконнекта
    tokio::spawn(async move {
        let internal_url = Url::parse("ws://localhost:8000/ws/internal").expect("Bad Internal URL");
        loop {
            match connect_async(internal_url.clone()).await {
                Ok((ws_stream, _)) => {
                    println!("Connected to Internal API");
                    let (mut write, mut read) = ws_stream.split();

                    // Важно: запускаем задачу на чтение, чтобы обрабатывать PING/PONG и не разрывать соединение
                    tokio::spawn(async move {
                        while let Some(_) = read.next().await {}
                    });

                    while let Some(msg) = rx.recv().await {
                        if let Err(e) = write.send(msg).await {
                            eprintln!("WS Send Error: {}. Reconnecting...", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to connect to Internal API: {}. Retrying in 5s...", e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });

    // Задача 2: Сборщик Funding Rates (раз в 15 секунд)
    let tx_funding = tx.clone();
    tokio::spawn(async move {
        // Добавляем User-Agent, чтобы биржи (особенно OKX) не блокировали запросы
        let client = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
            .build()
            .unwrap();

        loop {
            if let Err(e) = fetch_and_send_funding(&client, &tx_funding).await {
                eprintln!("Funding task error: {}", e);
            }
            sleep(Duration::from_secs(15)).await;
        }
    });

    // Разделяем поток на чтение и запись (хотя писать мы пока не будем)
    let (_, mut read) = ws_stream.split();

    // Хранилище состояний: Symbol -> Last Reference Price
    let mut scanner_state = ScannerState {
        history: HashMap::new(),
        cooldowns: HashMap::new(),
    };

    while let Some(message) = read.next().await {
        match message {
            Ok(msg) => {
                if let Message::Text(text) = msg {
                    // Парсим массив тикеров
                    match serde_json::from_str::<Vec<TickerEvent>>(&text) {
                        Ok(events) => {
                            // Фильтруем только USDT пары
                            let usdt_events: Vec<TickerEvent> = events.into_iter()
                                .filter(|e| e.s.ends_with("USDT"))
                                .collect();
                            
                            // 1. Ticker Tape (Top 15 by Volume)
                            let tape_msg = process_ticker_tape(&usdt_events);
                            if let Err(e) = tx.send(Message::Text(tape_msg.to_string())) {
                                eprintln!("Failed to send tape: {}", e);
                            }

                            // 2. Momentum Scanner
                            if let Some(mom_msg) = process_momentum(&usdt_events, &mut scanner_state) {
                                if let Err(e) = tx.send(Message::Text(mom_msg.to_string())) {
                                    eprintln!("Failed to send momentum: {}", e);
                                }
                            }
                        }
                        Err(e) => eprintln!("JSON parsing error: {}", e),
                    }
                }
            }
            Err(e) => eprintln!("WebSocket error: {}", e),
        }
    }
}

fn process_ticker_tape(events: &[TickerEvent]) -> serde_json::Value {
    let mut sorted: Vec<&TickerEvent> = events.iter().collect();
    // Сортировка по объему (q) по убыванию
    sorted.sort_by(|a, b| {
        let vol_a = a.q.parse::<f64>().unwrap_or(0.0);
        let vol_b = b.q.parse::<f64>().unwrap_or(0.0);
        vol_b.partial_cmp(&vol_a).unwrap_or(std::cmp::Ordering::Equal)
    });

    let top_15: Vec<_> = sorted.into_iter().take(15).map(|e| {
        json!({
            "symbol": e.s,
            "price": e.c.parse::<f64>().unwrap_or(0.0),
            "change": e.p.parse::<f64>().unwrap_or(0.0)
        })
    }).collect();

    json!({
        "type": "ticker_tape",
        "data": top_15
    })
}

fn process_momentum(events: &[TickerEvent], state: &mut ScannerState) -> Option<serde_json::Value> {
    let mut momentum_data = Vec::new();
    let now_instant = Instant::now();
    let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    
    let window_5m = 5 * 60 * 1000;   // 5 minutes in ms
    let window_2m = 2 * 60 * 1000;   // 2 minutes in ms
    let window_15s = 15 * 1000;      // 15 seconds in ms
    let cooldown_duration = Duration::from_secs(15 * 60); // 15 minutes cooldown

    for event in events {
        let current_price = event.c.parse::<f64>().unwrap_or(0.0);
        let current_q = event.q.parse::<f64>().unwrap_or(0.0);
        
        let history = state.history.entry(event.s.clone()).or_insert_with(VecDeque::new);
        
        // 1. Calculate Volume Delta
        let mut vol_delta = 0.0;
        if let Some(last_tick) = history.back() {
            vol_delta = current_q - last_tick.raw_q;
            if vol_delta < 0.0 { vol_delta = 0.0; } // Reset or anomaly
        }

        // 2. Add Tick
        history.push_back(TickData {
            price: current_price,
            volume: vol_delta,
            raw_q: current_q,
            timestamp: now_ts,
        });

        // 3. Prune old ticks (> 5 mins)
        while let Some(tick) = history.front() {
            if now_ts - tick.timestamp > window_5m {
                history.pop_front();
            } else {
                break;
            }
        }

        // 4. Check Cooldown
        if let Some(last_alert) = state.cooldowns.get(&event.s) {
            if now_instant.duration_since(*last_alert) < cooldown_duration {
                continue;
            }
        }
        
        // 5. Calculate Metrics
        let mut vol_15s = 0.0;
        let mut vol_2m = 0.0;
        let mut vol_5m = 0.0;

        for tick in history.iter() {
            let age = now_ts - tick.timestamp;
            if age <= window_5m { vol_5m += tick.volume; }
            if age <= window_2m { vol_2m += tick.volume; }
            if age <= window_15s { vol_15s += tick.volume; }
        }

        // Find reference prices
        let target_15s = now_ts.saturating_sub(window_15s);
        let target_2m = now_ts.saturating_sub(window_2m);
        
        let get_price_at = |target: u64| -> f64 {
            history.iter()
                .min_by_key(|t| (t.timestamp as i64 - target as i64).abs())
                .map(|t| t.price)
                .unwrap_or(current_price)
        };

        let price_15s_ago = get_price_at(target_15s);
        let price_2m_ago = get_price_at(target_2m);

        let change_15s = if price_15s_ago > 0.0 { (current_price - price_15s_ago) / price_15s_ago } else { 0.0 };
        let change_2m = if price_2m_ago > 0.0 { (current_price - price_2m_ago) / price_2m_ago } else { 0.0 };

        // Calculate Rates (Volume per second)
        let rate_15s = vol_15s / 15.0;
        let rate_2m = vol_2m / 120.0;
        
        // Calculate Norms (Average rate of the REST of the 5m window)
        // This compares current burst against background noise
        let norm_15s = if vol_5m > vol_15s { (vol_5m - vol_15s) / (300.0 - 15.0) } else { 0.0 };
        let norm_2m = if vol_5m > vol_2m { (vol_5m - vol_2m) / (300.0 - 120.0) } else { 0.0 };

        let mut signal_type = "";
        let mut msg = "";
        let mut direction = "NEUTRAL";
        let mut final_change = 0.0;

        // Logic 1: Manipulation (Micro-burst)
        // Volume rate 15s > 20x Background rate, Price > 1.5%
        let is_manipulation = if norm_15s > 0.0 { rate_15s > 20.0 * norm_15s } else { rate_15s > 0.0 };
        
        if is_manipulation && change_15s.abs() > 0.015 {
            signal_type = "MANIPULATION";
            msg = "Искусственный дамп/памп! Объем x20 за 15 сек.";
            direction = if change_15s > 0.0 { "PUMP" } else { "DUMP" };
            final_change = change_15s;
        }
        // Logic 2: Organic Breakout (Algo-trend)
        // Volume rate 2m > 4x Background rate, Price > 2%
        else {
            let is_trend = if norm_2m > 0.0 { rate_2m > 4.0 * norm_2m } else { rate_2m > 0.0 };
            
            if is_trend && change_2m.abs() > 0.02 {
                signal_type = "ORGANIC_TREND";
                msg = "Плавный рост объемов. Алгоритмы в деле.";
                direction = if change_2m > 0.0 { "PUMP" } else { "DUMP" };
                final_change = change_2m;
            }
        }

        if !signal_type.is_empty() {
            state.cooldowns.insert(event.s.clone(), now_instant);
            momentum_data.push(json!({
                "symbol": event.s,
                "signal": signal_type,
                "msg": msg,
                "direction": direction,
                "price": current_price,
                "change_pct": final_change * 100.0
            }));
        }
    }

    if momentum_data.is_empty() {
        None
    } else {
        Some(json!({
            "type": "momentum",
            "data": momentum_data
        }))
    }
}

// Логика сбора и анализа ставок финансирования
async fn fetch_and_send_funding(
    client: &reqwest::Client,
    tx: &mpsc::UnboundedSender<Message>,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Запрос к Binance Futures
    let binance_resp = match client.get("https://fapi.binance.com/fapi/v1/premiumIndex").send().await {
        Ok(resp) => {
            if resp.status().is_success() {
                resp.json::<Vec<BinanceFunding>>().await.unwrap_or_else(|e| {
                    eprintln!("Binance JSON error: {}", e);
                    Vec::new()
                })
            } else {
                eprintln!("Binance HTTP error: {}", resp.status());
                Vec::new()
            }
        }
        Err(e) => {
            eprintln!("Binance request error: {}", e);
            Vec::new()
        }
    };

    // 2. Запрос к Bybit Linear
    let bybit_resp = match client.get("https://api.bybit.com/v5/market/tickers?category=linear").send().await {
        Ok(resp) => {
            if resp.status().is_success() {
                resp.json::<BybitResponse>().await.ok()
            } else {
                eprintln!("Bybit HTTP error: {}", resp.status());
                None
            }
        }
        Err(e) => {
            eprintln!("Bybit request error: {}", e);
            None
        }
    };

    // 3. Запрос к OKX Swap
    let okx_resp = match client.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP").send().await {
        Ok(resp) => {
            if resp.status().is_success() {
                resp.json::<OkxResponse>().await.ok()
            } else {
                eprintln!("OKX HTTP error: {}", resp.status());
                None
            }
        }
        Err(e) => {
            eprintln!("OKX request error: {}", e);
            None
        }
    };

    // 4. Агрегация данных
    let mut rates_map: HashMap<String, Vec<(&str, f64)>> = HashMap::new();

    // Binance
    for item in binance_resp {
        if let Ok(rate) = item.last_funding_rate.parse::<f64>() {
            rates_map.entry(item.symbol).or_default().push(("Binance", rate));
        }
    }

    // Bybit
    if let Some(data) = bybit_resp {
        for item in data.result.list {
            if let Ok(rate) = item.funding_rate.parse::<f64>() {
                rates_map.entry(item.symbol).or_default().push(("Bybit", rate));
            }
        }
    }

    // OKX (формат BTC-USDT-SWAP -> BTCUSDT)
    if let Some(data) = okx_resp {
        for item in data.data {
            let symbol = item.inst_id.replace("-", "").replace("SWAP", "");
            if let Ok(rate) = item.funding_rate.parse::<f64>() {
                rates_map.entry(symbol).or_default().push(("OKX", rate));
            }
        }
    }

    // 5. Анализ спредов
    let mut opportunities = Vec::new();
    for (symbol, rates) in rates_map {
        if rates.len() < 2 || !symbol.ends_with("USDT") { continue; }

        // Находим биржу с макс. ставкой (для Short) и мин. ставкой (для Long)
        // Логика: Short получает фандинг, если он положительный. Long платит.
        // Арбитраж: Short там, где дорого (получаем много), Long там, где дешево (платим мало).
        let (max_exch, max_rate) = rates.iter().max_by(|a, b| a.1.partial_cmp(&b.1).unwrap()).unwrap();
        let (min_exch, min_rate) = rates.iter().min_by(|a, b| a.1.partial_cmp(&b.1).unwrap()).unwrap();

        let spread = (max_rate - min_rate).abs();

        if spread > 0.0 {
            opportunities.push(json!({
                "coin": symbol.replace("USDT", ""),
                "spread": spread,
                "exchange_short": max_exch,
                "exchange_long": min_exch,
                "rate_short": max_rate,
                "rate_long": min_rate
            }));
        }
    }

    // 6. Сортируем по убыванию спреда и берем топ-5
    opportunities.sort_by(|a, b| {
        let s_a = a["spread"].as_f64().unwrap_or(0.0);
        let s_b = b["spread"].as_f64().unwrap_or(0.0);
        s_b.partial_cmp(&s_a).unwrap()
    });
    let top_5: Vec<_> = opportunities.into_iter().take(5).collect();

    // 6. Отправляем в канал
    let msg = json!({ "type": "funding", "data": top_5 });
    tx.send(Message::Text(msg.to_string()))?;

    Ok(())
}
