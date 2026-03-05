#!/bin/bash

echo "🚀 Запускаем Python FastAPI..."
uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} &
PYTHON_PID=$!

echo "⏳ Ждем 5 секунд для инициализации БД и веб-сокетов..."
sleep 5

# Проверяем, жив ли процесс Питона
if ! kill -0 $PYTHON_PID 2>/dev/null; then
  echo "❌ ОШИБКА: Python-сервер упал сразу после запуска! Ищи Traceback выше в логах."
  exit 1
fi

echo "🦀 Запускаем Rust-движок..."
./rust_engine