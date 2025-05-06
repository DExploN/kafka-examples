#!/bin/bash

# Скрипт для создания топиков
echo "Создаем основной топик retry-topic"
docker exec -it kafka_examples_kafka kafka-topics --create \
    --topic retry-topic \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

echo "Создаем топик для повторной обработки retry-topic-retry"
docker exec -it kafka_examples_kafka kafka-topics --create \
    --topic retry-topic-retry \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

# Проверка созданных топиков
echo "Проверяем созданные топики"
docker exec -it kafka_examples_kafka kafka-topics --describe \
    --topic retry-topic \
    --bootstrap-server localhost:9092

docker exec -it kafka_examples_kafka kafka-topics --describe \
    --topic retry-topic-retry \
    --bootstrap-server localhost:9092
