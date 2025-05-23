# Пример работы с партициями в Apache Kafka на Golang

Этот пример демонстрирует работу с конкретными партициями в Apache Kafka вместо использования автоматического распределения сообщений.

## Описание

В данном примере показано, как:
1. Отправлять сообщения в конкретные партиции на основе ключа
2. Читать сообщения из определенной партиции

## Файлы примера

- `create-topic.sh` - скрипт для создания топика с несколькими партициями
- `producer.go` - отправляет сообщения в конкретные партиции на основе хеша ключа
- `consumer.go` - читает сообщения из указанной партиции

## Подготовка

Перед запуском примера необходимо создать топик с несколькими партициями:

```bash
chmod +x examples/partitioned/create-topic.sh
./examples/partitioned/create-topic.sh
```

## Запуск примера

### Запуск продюсера
```bash
docker exec -it kafka_examples_golang bash -c "cd examples/partitioned && go run producer.go"
```

### Запуск консьюмера для чтения из конкретной партиции
```bash
# Чтение из партиции 0 (по умолчанию)
docker exec -it kafka_examples_golang bash -c "cd examples/partitioned && go run consumer.go"

# Чтение из партиции 1
docker exec -it kafka_examples_golang bash -c "cd examples/partitioned && go run consumer.go 1"

# Чтение из партиции 2
docker exec -it kafka_examples_golang bash -c "cd examples/partitioned && go run consumer.go 2"
```

## Механизм работы

### Как работает распределение по партициям

1. Для каждого ключа вычисляется хеш (используется реализация CRC32)
2. Хеш преобразуется в положительное число
3. Берется остаток от деления на количество партиций (модуль)
4. Полученное число используется как номер партиции

Таким образом, сообщения с одинаковым ключом всегда попадают в одну и ту же партицию, что гарантирует сохранение порядка для каждого ключа.

## Сценарий использования

Этот пример подходит для:
- Систем, где требуется сохранение порядка сообщений для определенных ключей
- Приложений, где нужно гарантировать, что сообщения с одинаковым ключом всегда попадают в одну партицию
- Случаев, когда необходимо распределить нагрузку между несколькими консьюмерами, каждый из которых обрабатывает свою партицию