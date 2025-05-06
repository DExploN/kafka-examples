#!/bin/bash

# Скрипт для создания топика с несколькими партициями
docker exec -it kafka_examples_kafka kafka-topics --create \
    --topic partitioned-topic \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

# Проверка созданного топика
docker exec -it kafka_examples_kafka kafka-topics --describe \
    --topic partitioned-topic \
    --bootstrap-server localhost:9092
