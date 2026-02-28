use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque, HashSet};
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

    // Задача 3: MEXC CVD Scanner (HFT Exhaustion/Absorption)
    tokio::spawn(mexc_cvd_scanner(tx.clone()));

    // Разделяем поток на чтение и запись (хотя писать мы пока не будем)
    let (_, mut read) = ws_stream.split();

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

// --- MEXC SCANNER MODULE ---

#[derive(Deserialize)]
struct MexcTickerResponse {
    success: bool,
    data: Vec<MexcTicker>,
}

#[derive(Deserialize)]
struct MexcTicker {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: f64,
    #[serde(rename = "amount24")]
    amount_24: f64,
}

#[derive(Deserialize)]
struct MexcResponse {
    channel: Option<String>,
    symbol: Option<String>,
    data: Option<MexcData>,
}

#[derive(Deserialize)]
struct MexcData {
    p: f64,
    v: f64,
    #[serde(rename = "T")]
    side: i32, // 1: Buy, 2: Sell
    t: u64,
}

struct MexcTick {
    price: f64,
    volume: f64,
    side: i32,
    timestamp: u64,
}

struct MexcScannerState {
    history: HashMap<String, VecDeque<MexcTick>>,
    cooldowns: HashMap<String, Instant>,
    avg_vol: HashMap<String, f64>, // Скользящая средняя объема за 60 сек
}

async fn mexc_cvd_scanner(tx: mpsc::UnboundedSender<Message>) {
    let connect_addr = "wss://contract.mexc.com/edge";
    let url = Url::parse(connect_addr).expect("Bad MEXC URL");
    let http_client = reqwest::Client::new();
    
    println!("Connecting to MEXC Futures Scanner: {}", connect_addr);

    loop {
        match connect_async(url.clone()).await {
            Ok((mut ws_stream, _)) => {
                println!("Connected to MEXC WebSocket");
                
                let (mut ws_write, mut ws_read) = ws_stream.split();
                
                // Channels for internal communication
                let (ws_msg_tx, mut ws_msg_rx) = mpsc::unbounded_channel::<Message>();
                let (sniper_tx, mut sniper_rx) = mpsc::unbounded_channel::<String>();

                // Task: WebSocket Writer
                tokio::spawn(async move {
                    while let Some(msg) = ws_msg_rx.recv().await {
                        if ws_write.send(msg).await.is_err() { break; }
                    }
                });

                // Task: Hunter (Global Radar) - 3s Loop
                let http_client_clone = http_client.clone();
                let global_tx = tx.clone();
                
                tokio::spawn(async move {
                    let mut prev_state: HashMap<String, (f64, f64)> = HashMap::new(); // Symbol -> (Price, Vol)
                    
                    loop {
                        let url = "https://contract.mexc.com/api/v1/contract/ticker";
                        if let Ok(resp) = http_client_clone.get(url).send().await {
                            if let Ok(json) = resp.json::<MexcTickerResponse>().await {
                                if json.success {
                                    for t in json.data {
                                        if t.symbol == "BTC_USDT" || t.symbol == "ETH_USDT" { continue; }
                                        
                                        // Базовый фильтр ликвидности
                                        if t.amount_24 < 50_000.0 { continue; }

                                        if let Some((old_price, old_vol)) = prev_state.get(&t.symbol) {
                                            let pct_change = (t.last_price - old_price) / old_price;
                                            let volume_delta = t.amount_24 - old_vol;
                                            
                                            // Игнорируем отрицательную дельту (сброс суток)
                                            if volume_delta >= 0.0 {
                                                // Условие Вспышки: Влили > $15k И Цена сдвинулась > 1.2%
                                                if volume_delta > 15_000.0 && pct_change.abs() > 0.012 {
                                                    let msg = json!({
                                                        "type": "momentum",
                                                        "data": [{
                                                            "symbol": t.symbol,
                                                            "signal": "SUDDEN_BREAKOUT",
                                                            "change_pct": pct_change * 100.0,
                                                            "msg": format!("Вспышка! ${:.0} за 3с, Изм: {:.2}%", volume_delta, pct_change * 100.0)
                                                        }]
                                                    });
                                                    let _ = global_tx.send(Message::Text(msg.to_string()));
                                                    let _ = sniper_tx.send(t.symbol.clone());
                                                }
                                            }
                                        }
                                        prev_state.insert(t.symbol, (t.last_price, t.amount_24));
                                    }
                                }
                            }
                        }
                        sleep(Duration::from_secs(3)).await;
                    }
                });

                // Main Loop: Reader & Processor
                let mut state = MexcScannerState {
                    history: HashMap::new(),
                    cooldowns: HashMap::new(),
                    avg_vol: HashMap::new(),
                };
                
                let mut active_subs: VecDeque<String> = VecDeque::new();
                let mut subscribed_set: HashSet<String> = HashSet::new();
                
                let ws_msg_tx_pong = ws_msg_tx.clone();

                loop {
                    tokio::select! {
                        Some(msg) = ws_read.next() => {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if text.contains("ping") {
                                        let _ = ws_msg_tx_pong.send(Message::Text(json!({"method": "pong"}).to_string()));
                                        continue;
                                    }
                                    if let Ok(event) = serde_json::from_str::<MexcResponse>(&text) {
                                        if let (Some(symbol), Some(data)) = (event.symbol, event.data) {
                                            process_mexc_tick(&symbol, data, &mut state, &tx).await;
                                        }
                                    }
                                }
                                Ok(_) => {},
                                Err(_) => break,
                            }
                        }
                        Some(target) = sniper_rx.recv() => {
                            if !subscribed_set.contains(&target) {
                                // Evict if full (Max 20)
                                if active_subs.len() >= 20 {
                                    if let Some(old) = active_subs.pop_front() {
                                        subscribed_set.remove(&old);
                                        let msg = json!({ "method": "unsub.deal", "param": { "symbol": old } });
                                        let _ = ws_msg_tx_pong.send(Message::Text(msg.to_string()));
                                        
                                        // Clean state
                                        state.history.remove(&old);
                                        state.cooldowns.remove(&old);
                                        state.avg_vol.remove(&old);
                                        println!("MEXC Killer: Dropped cold target {}", old);
                                    }
                                }
                                
                                // Add new target
                                active_subs.push_back(target.clone());
                                subscribed_set.insert(target.clone());
                                let msg = json!({ "method": "sub.deal", "param": { "symbol": target } });
                                let _ = ws_msg_tx_pong.send(Message::Text(msg.to_string()));
                                println!("MEXC Killer: Locked on target {}", target);
                            }
                        }
                        else => break,
                    }
                }
            }
            Err(e) => {
                eprintln!("MEXC Connect Error: {}. Retrying in 5s...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn process_mexc_tick(symbol: &str, data: MexcData, state: &mut MexcScannerState, tx: &mpsc::UnboundedSender<Message>) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let history = state.history.entry(symbol.to_string()).or_default();
    
    history.push_back(MexcTick { price: data.p, volume: data.v, side: data.side, timestamp: now });

    // Окно 60 секунд
    while let Some(tick) = history.front() {
        if now - tick.timestamp > 60_000 { history.pop_front(); } else { break; }
    }

    if history.len() < 10 { return; } // Накопление данных

    // Кулдаун 15 минут
    if let Some(last) = state.cooldowns.get(symbol) {
        if Instant::now().duration_since(*last) < Duration::from_secs(15 * 60) { return; }
    }

    // Расчет метрик
    let start_price = history.front().unwrap().price;
    let end_price = history.back().unwrap().price;
    let price_change_pct = (end_price - start_price) / start_price;

    let (mut buy_vol, mut sell_vol) = (0.0, 0.0);
    for t in history.iter() { if t.side == 1 { buy_vol += t.volume; } else { sell_vol += t.volume; } }
    
    let total_vol = buy_vol + sell_vol;
    let delta = buy_vol - sell_vol;

    // Обновляем средний объем (экспоненциальное сглаживание)
    let avg = state.avg_vol.entry(symbol.to_string()).or_insert(total_vol);
    *avg = *avg * 0.99 + total_vol * 0.01;
    if *avg < 10.0 { return; } // Фильтр шума

    let mut signal = "";
    // Сигнал 1: Памп/Дамп (Объем x5, Цена > 2%)
    if total_vol > 5.0 * *avg && price_change_pct.abs() > 0.02 {
        signal = if price_change_pct > 0.0 { "MEXC_PUMP" } else { "MEXC_DUMP" };
    }
    // Сигнал 2: Истощение (Buy Vol x5, Delta > 0, Цена стоит)
    else if buy_vol > 5.0 * (*avg / 2.0) && delta > 0.0 && price_change_pct.abs() < 0.002 {
        signal = "EXHAUSTION_SHORT";
    }
    // Сигнал 3: Поглощение (Sell Vol x5, Delta < 0, Цена стоит)
    else if sell_vol > 5.0 * (*avg / 2.0) && delta < 0.0 && price_change_pct.abs() < 0.002 {
        signal = "ABSORPTION_LONG";
    }

    if !signal.is_empty() {
        state.cooldowns.insert(symbol.to_string(), Instant::now());
        let msg = json!({
            "type": "momentum", // Используем тип momentum для совместимости с Python
            "data": [{
                "symbol": symbol,
                "signal": signal,
                "price": end_price,
                "change_pct": price_change_pct * 100.0,
                "msg": format!("MEXC HFT: {} (Vol x{:.1})", signal, total_vol / *avg)
            }]
        });
        let _ = tx.send(Message::Text(msg.to_string()));
        println!("🔥 MEXC Signal: {} on {}", signal, symbol);
    }
}
