# Примеры работы с Apache Kafka в Golang

Данный репозиторий содержит примеры использования Apache Kafka с Golang для различных сценариев.

## Структура проекта

```
golang/
├── src/
│   └── kafka/              # Основные пакеты для работы с Kafka
│       ├── producer.go     # Реализация продюсера
│       └── consumer.go     # Реализация консьюмера
├── examples/               # Примеры использования
│   ├── basic/              # Базовый пример
│   ├── advanced/           # Продвинутый пример с использованием
│   ├── partitioned/        # Пример с партиционированием
│   ├── retry-by-time/      # Пример механизма повторной обработки
│   ├── reread-by-time/     # Пример повторного чтения по временному интервалу
│   └── streams-and-ktable/ # Пример работы с потоками и таблицами
└── go.mod                  # Определение модуля и зависимостей
```

## Запуск примеров

Все примеры можно запустить внутри Docker-контейнера:

```bash
# Сборка и запуск контейнера
docker-compose up -d golang

# Вход в контейнер
docker exec -it kafka_examples_golang bash

# Запуск примера (внутри контейнера)
cd examples/basic
go run producer.go
go run consumer.go
```

## Доступные примеры

1. **basic** - Простой пример отправки и получения сообщений
2. **advanced** - Продвинутый пример с настройками, обработкой ошибок и подтверждениями
3. **partitioned** - Пример работы с конкретными партициями
4. **retry-by-time** - Реализация механизма повторной обработки сообщений
5. **reread-by-time** - Пример повторного чтения сообщений за указанный временной интервал
6. **streams-and-ktable** - Пример обработки потоков и создания таблиц данных

## Особенности реализации

В отличие от официального Kafka Streams API (доступного только для Java), 
в Go мы используем сочетание базовых операций Kafka и дополнительной логики для реализации 
аналогичного функционала потоковой обработки и табличных представлений.

## Технические детали

Примеры используют следующие библиотеки:

- `github.com/confluentinc/confluent-kafka-go/v2/kafka` - официальная библиотека от Confluent
- `github.com/segmentio/kafka-go` - альтернативная Go-native библиотека для некоторых сценариев