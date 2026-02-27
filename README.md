# 📈 Crypto Bloomberg Terminal (HFT Architecture MVP)

A high-performance, real-time cryptocurrency trading terminal inspired by the Bloomberg Terminal. Built with a microservices architecture to ensure zero-latency data processing and a seamless user experience.

## 🏗 Architecture Overview

This project abandons heavy frontend frameworks in favor of a raw, speed-optimized stack:

* **Data Engine (Rust):** A high-performance background worker built with `tokio` and `tungstenite`. It connects directly to the Binance USDT-M Futures WebSockets, parses tick-by-tick data, calculates momentum anomalies, and computes cross-exchange funding rate arbitrage in real-time.
* **WebSocket Bridge (Python / FastAPI):** Acts as a lightweight router. It receives processed, clean signals from the Rust engine via internal WebSockets and broadcasts them to all connected browser clients.
* **Frontend (Vanilla JS / HTML / CSS):** A strict, zero-overhead UI using CSS Grid for a dense, terminal-like layout. Real-time DOM manipulation ensures instant updates without the React re-rendering lifecycle penalty.

## ✨ Key Features

* **Real-time Ticker Tape:** Live streaming of top 15 USDT-M Futures pairs by volume.
* **Momentum Scanner:** A custom rolling-window algorithm (Rust) that detects sudden volume spikes and price anomalies (>0.5% in 60s), filtering out market noise to signal real PUMP/DUMP events.
* **Funding Rate Arbitrage:** Scans and calculates the spread between perpetual futures funding rates across exchanges to find delta-neutral opportunities.
* **Interactive Charts:** Native integration of TradingView Lightweight Charts directly connected to Binance klines stream for 1m timeframe accuracy.

## 🛠 Tech Stack

* **Engine:** Rust, Tokio, Tungstenite, Reqwest, Serde.
* **Backend:** Python 3, FastAPI, Uvicorn, Websockets.
* **Frontend:** Vanilla JavaScript, HTML5, CSS Grid, TradingView Lightweight Charts.

## 🚀 How to Run Locally

You will need two separate terminals to run the microservices.

**1. Start the Python WebSocket Bridge & UI Server:**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
pip install -r requirements.txt
cd backend_python
uvicorn main:app --reload
```

**2. Start the Rust Data Engine:**
```bash
cd engine_rust
cargo run 
```

**3. Open the Terminal:**
Navigate to http://localhost:8000 in your browser.

Built by Detroth(https://github.com/Detroth) - 2026