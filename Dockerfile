# ==========================================
# ЭТАП 1: Сборка Rust-ядра
# ==========================================
FROM rust:slim as rust-builder

WORKDIR /build
# Копируем только папку с Rust
COPY engine_rust/ ./engine_rust/

WORKDIR /build/engine_rust
# ВАЖНО: Убедись, что после компиляции файл называется именно так, как указано ниже.
# Имя файла берется из поля `name` в твоем Cargo.toml. 
# Если твой проект называется "engine_rust", то бинарник будет лежать в target/release/engine_rust
RUN cargo build --release

# ==========================================
# ЭТАП 2: Python и финальная сборка
# ==========================================
FROM python:3.11-slim

WORKDIR /app

# Копируем зависимости Python и устанавливаем их
COPY backend_python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код Питона
COPY backend_python/ .

# Копируем скомпилированный бинарник Раста из первого этапа
# ВНИМАНИЕ: Замени "ИМЯ_ТВОЕГО_ПРОЕКТА" на то, что написано в Cargo.toml (например, engine_rust)
COPY --from=rust-builder /build/engine_rust/target/release/engine_rust ./rust_engine

# Копируем скрипт старта из корня
COPY start.sh .

# Даем права на запуск скрипта и бинарника
RUN chmod +x start.sh ./rust_engine

# Запускаем оркестратор
CMD ["./start.sh"]