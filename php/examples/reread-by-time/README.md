# Пример повторного чтения сообщений Kafka по временному интервалу

Этот пример демонстрирует, как реализовать повторное чтение сообщений из Kafka за определенный временной интервал, не нарушая основной поток чтения. Это полезно, когда вы обнаружили ошибку в логике обработки и хотите повторно обработать сообщения за определенный период, но при этом продолжать обрабатывать новые сообщения с текущей позиции.

## Описание

В данном примере реализована следующая схема работы:

1. **Продюсер** отправляет сообщения с метками времени (`timestamp`).
2. **Основной консьюмер** читает и обрабатывает сообщения, одновременно сохраняя карту соответствия между временем сообщения и его смещением (offset) в Kafka.
3. **Консьюмер повторного чтения** использует эту карту для поиска и повторной обработки сообщений за указанный временной интервал, не влияя на позицию основного консьюмера.

## Файлы примера

- **producer.php** - отправляет сообщения в Kafka с метками времени
- **consumer.php** - основной консьюмер, который читает сообщения и сохраняет карту смещений
- **reread-consumer.php** - консьюмер для повторного чтения сообщений за указанный временной интервал
- **create-topic.sh** - скрипт для создания необходимого топика

## Структура сообщения

Каждое сообщение имеет следующую структуру:

```json
{
  "id": "уникальный_идентификатор",
  "message": "текст_сообщения",
  "timestamp": 1620000000,  // Unix timestamp события
  "value": 42               // Полезные данные
}
```

## Механизм работы

### 1. Сохранение карты смещений

Основной консьюмер при чтении сообщений сохраняет информацию о соответствии между временем сообщения и его смещением в Kafka:

```php
$offsetTimeMap[$partition][] = [
    'offset' => $offset,
    'timestamp' => $timestamp,
    'datetime' => date('Y-m-d H:i:s', $timestamp)
];
```

Эта информация сохраняется в JSON-файл, который можно использовать для повторного чтения.

### 2. Повторное чтение по времени

Для повторного чтения сообщений за определенный временной интервал:

1. Загружается карта смещений из файла
2. Находятся все смещения, соответствующие заданному временному интервалу
3. Для каждого найденного смещения выполняется точечное чтение из Kafka
4. Сообщения обрабатываются повторно

## Преимущества подхода

1. **Независимость от основного потока** - повторное чтение не влияет на позицию основного консьюмера
2. **Точность** - можно точно указать временной интервал для повторной обработки
3. **Гибкость** - можно многократно перечитывать разные временные интервалы
4. **Минимальное влияние на производительность** - читаются только нужные сообщения, а не весь поток

## Подготовка

Перед запуском примера необходимо создать топик:

```bash
# Создание топика
./create-topic.sh
```

## Запуск примера

### 1. Запуск продюсера для генерации сообщений
```bash
docker exec -it kafka_examples_php php /var/www/html/examples/reread-by-time/producer.php
```

### 2. Запуск основного консьюмера
```bash
docker exec -it kafka_examples_php php /var/www/html/examples/reread-by-time/consumer.php
```

### 3. Запуск консьюмера для повторного чтения за указанный период
```bash
docker exec -it kafka_examples_php php /var/www/html/examples/reread-by-time/reread-consumer.php '2025-05-06 14:00:00' '2025-05-06 14:10:00'
```

## Практические рекомендации

1. В продакшн-системах рекомендуется хранить карту смещений в базе данных, а не в файле, для обеспечения надежности и масштабируемости.
2. Для больших объемов данных можно оптимизировать хранение, используя интервалы смещений вместо отдельных значений.
3. Можно добавить дополнительные метаданные к сообщениям, такие как идентификатор транзакции или бизнес-ключ, для более гибкого поиска.
4. При работе с большими временными интервалами рекомендуется обрабатывать смещения пакетами, чтобы не перегружать систему.

## Сравнение с другими примерами

- **basic** - простой пример работы с Kafka без дополнительных механизмов
- **advanced** - продвинутый пример с использованием ООП-подхода
- **partitioned** - пример с явным назначением партиций
- **lowlevel** - пример с использованием низкоуровневого API
- **retry-by-time** - пример с механизмом повторной обработки на основе времени
- **reread-by-time** (этот пример) - пример с повторным чтением сообщений за указанный временной интервал
