# Примеры работы с Apache Kafka на PHP

Этот проект содержит примеры работы с Apache Kafka на PHP с использованием расширения rdkafka.

## Структура проекта

```
/php/                  - PHP-код и примеры
  /src/                - Классы для работы с Kafka
  /examples/           - Примеры работы с Kafka
    /basic/            - Базовые примеры
      producer.php     - Базовый продюсер
      consumer.php     - Базовый консьюмер
      README.md        - Описание базовых примеров
    /advanced/         - Продвинутые примеры
      producer.php     - Продвинутый продюсер
      consumer.php     - Продвинутый консьюмер
      README.md        - Описание продвинутых примеров
/docker/               - Файлы для Docker
  /php/                - Конфигурация PHP
    Dockerfile         - Dockerfile для PHP
docker-compose.yml     - Конфигурация Docker Compose
.env                   - Переменные окружения
```

## Требования

- Docker
- Docker Compose

## Настройка пользователя

В файле `.env` вы можете настроить пользователя для PHP контейнера:

```
PHP_USER=phpuser  # Имя пользователя
PHP_UID=1000      # UID пользователя (должен совпадать с вашим UID в хост-системе)
PHP_GID=1000      # GID группы (должен совпадать с вашим GID в хост-системе)
```

Чтобы узнать ваш UID и GID в хост-системе, выполните команду:

```bash
id -u && id -g
```

## Запуск проекта

### 1. Клонирование репозитория

```bash
git clone <url-репозитория>
cd kafka-examples
```

### 2. Запуск Docker-контейнеров

```bash
docker-compose up -d
```

Это запустит:
- Zookeeper (управление кластером Kafka)
- Kafka (брокер сообщений)
- PHP (контейнер с PHP и расширением rdkafka)

### 3. Установка зависимостей PHP

```bash
docker exec -it kafka_examples_php composer install -d /var/www/html
```

## Использование примеров

### Примеры работы с Kafka

Все примеры разделены на базовые и продвинутые. Каждый пример содержит свой README с подробным описанием.

#### Базовый продюсер

Отправляет несколько сообщений в топик `test-topic`:

```bash
docker exec -it kafka_examples_php php /var/www/html/examples/basic/producer.php
```

#### Базовый консьюмер

Читает сообщения из топика `test-topic`:

```bash
docker exec -it kafka_examples_php php /var/www/html/examples/basic/consumer.php
```

#### Продвинутый продюсер

Использует класс `KafkaProducer` для отправки сообщений в топик `advanced-topic`:

```bash
docker exec -it kafka_examples_php php /var/www/html/examples/advanced/producer.php
```

#### Продвинутый консьюмер

Использует класс `KafkaConsumer` для чтения сообщений из топиков `advanced-topic` и `test-topic`:

```bash
docker exec -it kafka_examples_php php /var/www/html/examples/advanced/consumer.php
```

Подробное описание каждого примера и различия между ними можно найти в соответствующих README файлах в папках примеров.

## Работа с топиками

### Веб-интерфейс AKHQ

После запуска контейнеров вы можете получить доступ к веб-интерфейсу AKHQ для управления Kafka по адресу:

```
http://localhost:8080
```

С помощью AKHQ вы можете:
- Просматривать и создавать топики
- Просматривать сообщения в топиках
- Отправлять новые сообщения
- Мониторить группы потребителей
- Просматривать информацию о брокерах

### Создание нового топика через командную строку

```bash
docker exec -it kafka_examples_kafka kafka-topics --create --topic new-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Список топиков через командную строку

```bash
docker exec -it kafka_examples_kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Просмотр сообщений в топике через командную строку

```bash
docker exec -it kafka_examples_kafka kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092
```

## Остановка проекта

```bash
docker-compose down
```

## Дополнительная информация

- Kafka доступна по адресу `localhost:9092`
- Внутри Docker-сети Kafka доступна по адресу `kafka:29092`
- Все сообщения сохраняются в топиках Kafka, пока контейнеры запущены
