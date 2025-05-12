FROM golang:1.21-bullseye

# Установка необходимых инструментов
RUN apt-get update && apt-get install -y git gcc libc-dev make bash && rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /app

# Копируем go.mod и go.sum (если существуют)
COPY ./golang/go.mod ./golang/go.sum* ./

# Предварительно загружаем зависимости
RUN go mod download

# Устанавливаем зависимости (опционально, go mod download уже достаточно)
RUN go get github.com/segmentio/kafka-go
RUN go get github.com/confluentinc/confluent-kafka-go/v2/kafka

# Копируем исходный код
COPY ./golang ./

# Экспонируем порты если необходимо
EXPOSE 8080

# Команда по умолчанию - запуск оболочки
CMD ["tail", "-f", "/dev/null"]