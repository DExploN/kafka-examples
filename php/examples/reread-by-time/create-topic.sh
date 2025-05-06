#!/bin/bash

# Скрипт для создания топика
echo "Создаем топик reread-topic"
docker exec -it kafka_examples_kafka kafka-topics --create \
    --topic reread-topic \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

# Проверка созданного топика
echo "Проверяем созданный топик"
docker exec -it kafka_examples_kafka kafka-topics --describe \
    --topic reread-topic \
    --bootstrap-server localhost:9092
