use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
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

struct MomentumState {
    history: VecDeque<(Instant, f64)>,
    last_alert: Instant,
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
                    let (mut write, _) = ws_stream.split();
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
    let mut momentum_cache: HashMap<String, MomentumState> = HashMap::new();

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
                            if let Some(mom_msg) = process_momentum(&usdt_events, &mut momentum_cache) {
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

fn process_momentum(events: &[TickerEvent], cache: &mut HashMap<String, MomentumState>) -> Option<serde_json::Value> {
    let mut momentum_data = Vec::new();
    let now = Instant::now();
    let window_duration = Duration::from_secs(60); // Окно 1 минута
    let cooldown = Duration::from_secs(30); // Не спамить чаще чем раз в 30 сек

    for event in events {
        let current_price = event.c.parse::<f64>().unwrap_or(0.0);
        
        // Получаем или создаем состояние для тикера
        let state = cache.entry(event.s.clone()).or_insert(MomentumState {
            history: VecDeque::new(),
            last_alert: now - cooldown, // Готов к алерту сразу
        });

        // 1. Добавляем текущую цену в историю
        state.history.push_back((now, current_price));

        // 2. Удаляем старые записи (старше window_duration)
        while let Some(&(time, _)) = state.history.front() {
            if now.duration_since(time) > window_duration {
                state.history.pop_front();
            } else {
                break;
            }
        }
        
        let mut triggered = false;
        let mut direction = "NEUTRAL";
        let mut change_pct = 0.0;

        // 3. Сравниваем с самой старой ценой в окне (начало окна)
        if let Some(&(_, start_price)) = state.history.front() {
            change_pct = (current_price - start_price) / start_price;
            
            // Если изменение > 0.5% и прошел кулдаун
            if change_pct.abs() > 0.005 && now.duration_since(state.last_alert) > cooldown {
                triggered = true;
                direction = if change_pct > 0.0 { "PUMP" } else { "DUMP" };
                state.last_alert = now;
            }
        }

        if triggered {
            momentum_data.push(json!({
                "symbol": event.s,
                "direction": direction,
                "price": current_price,
                "change_pct": change_pct * 100.0
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
