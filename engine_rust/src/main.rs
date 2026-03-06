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
        let port = std::env::var("PORT").unwrap_or_else(|_| "8000".to_string());
        let internal_ws_url = format!("ws://127.0.0.1:{}/ws/internal", port);
        let internal_url = Url::parse(&internal_ws_url).expect("Bad Internal URL");
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
    #[serde(rename = "riseFallRate")]
    rise_fall_rate: f64,
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

#[derive(Debug, Clone)]
struct TickData {
    timestamp: u64, // Minute timestamp (Unix ms / 60000)
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

struct MexcScannerState {
    history: HashMap<String, VecDeque<TickData>>,
    current_candles: HashMap<String, TickData>,
    cooldowns: HashMap<(String, String), Instant>,
    daily_changes: HashMap<String, f64>,
    daily_volumes: HashMap<String, f64>,
}

enum ScannerControl {
    MarketUpdate { targets: Vec<String>, daily_changes: HashMap<String, f64>, daily_volumes: HashMap<String, f64> },
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
                let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel::<ScannerControl>();

                // Task: WebSocket Writer
                tokio::spawn(async move {
                    while let Some(msg) = ws_msg_rx.recv().await {
                        if ws_write.send(msg).await.is_err() { break; }
                    }
                });

                // Task: Hunter (Global Radar) - 10s Loop (Less aggressive than 3s, focus on analysis)
                let http_client_clone = http_client.clone();
                
                tokio::spawn(async move {
                    loop {
                        let url = "https://contract.mexc.com/api/v1/contract/ticker";
                        if let Ok(resp) = http_client_clone.get(url).send().await {
                            if let Ok(json) = resp.json::<MexcTickerResponse>().await {
                                if json.success {
                                    let mut targets = Vec::new();
                                    let mut daily_map = HashMap::new();
                                    let mut vol_map = HashMap::new();
                                    
                                    // Filter liquid pairs
                                    let mut tickers = json.data;
                                    tickers.retain(|t| t.amount_24 >= 50_000.0 && t.symbol != "BTC_USDT" && t.symbol != "ETH_USDT");
                                    tickers.sort_by(|a, b| b.amount_24.partial_cmp(&a.amount_24).unwrap_or(std::cmp::Ordering::Equal));
                                    
                                    // Populate daily_map from tickers (before filtering/taking top 40 if needed, or just from filtered)
                                    // Since we moved json.data to tickers, we iterate tickers.
                                    for t in &tickers {
                                        daily_map.insert(t.symbol.clone(), t.rise_fall_rate);
                                        vol_map.insert(t.symbol.clone(), t.amount_24);
                                    }
                                    
                                    for t in tickers.iter() {
                                        targets.push(t.symbol.clone());
                                    }

                                    let _ = ctrl_tx.send(ScannerControl::MarketUpdate { 
                                        targets, 
                                        daily_changes: daily_map,
                                        daily_volumes: vol_map
                                    });
                                }
                            }
                        }
                        sleep(Duration::from_secs(10)).await;
                    }
                });

                // Main Loop: Reader & Processor
                let mut state = MexcScannerState {
                    history: HashMap::new(),
                    current_candles: HashMap::new(),
                    cooldowns: HashMap::new(),
                    daily_changes: HashMap::new(),
                    daily_volumes: HashMap::new(),
                };
                
                let mut subscribed_set: HashSet<String> = HashSet::new();
                
                let ws_msg_tx_pong = ws_msg_tx.clone();

                // PING task to keep connection alive
                let ws_msg_tx_ping = ws_msg_tx.clone();
                tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_secs(20)).await;
                        let _ = ws_msg_tx_ping.send(Message::Text(json!({"method": "ping"}).to_string()));
                    }
                });

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
                                            process_mexc_candle(&symbol, data, &mut state, &tx).await;
                                        }
                                    }
                                }
                                Ok(_) => {},
                                Err(_) => break,
                            }
                        }
                        Some(ctrl) = ctrl_rx.recv() => {
                            match ctrl {
                                ScannerControl::MarketUpdate { targets, daily_changes, daily_volumes } => {
                                    state.daily_changes = daily_changes;
                                    state.daily_volumes = daily_volumes;
                                    let new_set: HashSet<String> = targets.into_iter().collect();
                                    
                                    // Subscribe new
                                    for pair in &new_set {
                                        if !subscribed_set.contains(pair) {
                                            let msg = json!({ "method": "sub.deal", "param": { "symbol": pair } });
                                            let _ = ws_msg_tx_pong.send(Message::Text(msg.to_string()));
                                            println!("MEXC Subscribing: {}", pair);
                                        }
                                    }
                                    
                                    // Unsubscribe old
                                    let to_remove: Vec<String> = subscribed_set.iter()
                                        .filter(|p| !new_set.contains(*p))
                                        .cloned()
                                        .collect();
                                        
                                    for pair in to_remove {
                                        let msg = json!({ "method": "unsub.deal", "param": { "symbol": pair } });
                                        let _ = ws_msg_tx_pong.send(Message::Text(msg.to_string()));
                                        
                                        state.history.remove(&pair);
                                        state.current_candles.remove(&pair);
                                    }
                                    
                                    subscribed_set = new_set;
                                }
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

async fn process_mexc_candle(symbol: &str, data: MexcData, state: &mut MexcScannerState, tx: &mpsc::UnboundedSender<Message>) {
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let current_minute = now_ms / 60_000;

    let candle = state.current_candles.entry(symbol.to_string()).or_insert(TickData {
        timestamp: current_minute,
        open: data.p,
        high: data.p,
        low: data.p,
        close: data.p,
        volume: 0.0,
    });

    if candle.timestamp != current_minute {
        let finalized_candle = candle.clone();
        
        let history = state.history.entry(symbol.to_string()).or_default();
        history.push_back(finalized_candle);
        if history.len() > 30 { history.pop_front(); }

        let vol_24h = state.daily_volumes.get(symbol).cloned().unwrap_or(0.0);
        analyze_history(symbol, history, &mut state.cooldowns, vol_24h, tx).await;

        *candle = TickData {
            timestamp: current_minute,
            open: data.p,
            high: data.p,
            low: data.p,
            close: data.p,
            volume: 0.0,
        };
    }

    if data.p > candle.high { candle.high = data.p; }
    if data.p < candle.low { candle.low = data.p; }
    candle.close = data.p;
    candle.volume += data.v;
}

async fn analyze_history(symbol: &str, history: &VecDeque<TickData>, cooldowns: &mut HashMap<(String, String), Instant>, vol_24h: f64, tx: &mpsc::UnboundedSender<Message>) {
    if history.len() < 2 { return; }

    let now_instant = Instant::now();
    let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    
    // --- STRATEGY 1: SPLASH (Global Inefficiency) ---
    // Window: 30 mins (Full history)
    let mut min_p_30 = f64::MAX;
    let mut max_p_30 = f64::MIN;
    for candle in history.iter() {
        if candle.low < min_p_30 { min_p_30 = candle.low; }
        if candle.high > max_p_30 { max_p_30 = candle.high; }
    }

    if min_p_30 > 0.0 {
        let growth_30 = (max_p_30 - min_p_30) / min_p_30;
        if growth_30 >= 0.09 {
            let key = (symbol.to_string(), "SPLASH".to_string());
            let on_cooldown = cooldowns.get(&key).map(|t| now_instant.duration_since(*t) < Duration::from_secs(60 * 60)).unwrap_or(false);
            
            if !on_cooldown {
                cooldowns.insert(key, now_instant);
                let msg = json!({
                    "type": "momentum",
                    "data": [{
                        "symbol": symbol,
                        "signal_type": "SPLASH",
                        "current_price": history.back().unwrap().close,
                        "fair_price": (max_p_30 + min_p_30) / 2.0,
                        "change_pct": growth_30 * 100.0,
                        "volume_24h": vol_24h,
                        "timestamp": now_unix
                    }]
                });
                let _ = tx.send(Message::Text(msg.to_string()));
            }
        }
    }

    // --- STRATEGY 2: ADVANCED (Algo Reversion) ---
    // Window: 15 mins
    let start_idx = history.len().saturating_sub(15);
    let window_iter = history.iter().skip(start_idx);

    let mut min_price = f64::MAX;
    let mut max_price = f64::MIN;
    let mut min_time = 0;
    let mut max_time = 0;

    for candle in window_iter {
        if candle.low < min_price {
            min_price = candle.low;
            min_time = candle.timestamp;
        }
        if candle.high > max_price {
            max_price = candle.high;
            max_time = candle.timestamp;
        }
    }

    if min_price == 0.0 || min_price == f64::MAX { return; }

    let growth_pct = (max_price - min_price) / min_price;
    if growth_pct <= 0.04 || growth_pct >= 0.15 { return; }

    if max_time <= min_time || (max_time - min_time) < 3 { return; }

    let current_price = history.back().unwrap().close;
    if current_price >= max_price * 0.99 { return; }

    let signal_key = (symbol.to_string(), "ADVANCED".to_string());
    let on_cooldown = cooldowns.get(&signal_key).map(|t| now_instant.duration_since(*t) < Duration::from_secs(60 * 60)).unwrap_or(false);

    if !on_cooldown {
        let take_profit = max_price - ((max_price - min_price) * 0.5);
        let stop_loss = max_price * 1.002;
        let rr_ratio = if stop_loss - current_price != 0.0 { (current_price - take_profit) / (stop_loss - current_price) } else { 0.0 };

        cooldowns.insert(signal_key, now_instant);
        let msg = json!({
            "type": "momentum",
            "data": [{
                "symbol": symbol,
                "signal_type": "ADVANCED",
                "current_price": current_price,
                "fair_price": take_profit,
                "change_pct": growth_pct * 100.0,
                "volume_24h": vol_24h,
                "timestamp": now_unix
            }]
        });
        let _ = tx.send(Message::Text(msg.to_string()));
        println!("📉 ADVANCED: {}", symbol);
    }
}
