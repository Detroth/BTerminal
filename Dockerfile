# ==========================================
# ЭТАП 1: Сборка Rust-ядра
# ==========================================
FROM rust:slim as rust-builder

WORKDIR /build

# Устанавливаем системные зависимости для компиляции криптографии и HTTPS
RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential

# Копируем только папку с Rust
COPY engine_rust/ ./engine_rust/

WORKDIR /build/engine_rust
# Собираем бинарник
RUN cargo build --release

# ==========================================
# ЭТАП 2: Python и финальная сборка
# ==========================================
FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости для работы HTTPS в рантайме
RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*

# Копируем зависимости Python и устанавливаем их
COPY backend_python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код Питона
COPY backend_python/ .

# Копируем бинарник (ПРИМЕЧАНИЕ: замени mexc_scanner на имя из твоего Cargo.toml)
COPY --from=rust-builder /build/engine_rust/target/release/engine_rust ./rust_engine

# Копируем скрипт старта из корня
COPY start.sh .

RUN chmod +x start.sh ./rust_engine

CMD ["./start.sh"]