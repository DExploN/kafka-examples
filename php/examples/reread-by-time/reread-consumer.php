<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('reread-time-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'reread-topic';

// Получаем временной интервал из аргументов командной строки
if (count($argv) < 3) {
    $logger->error("Необходимо указать начальное и конечное время в формате YYYY-MM-DD HH:MM:SS");
    $logger->error("Пример: php reread-consumer.php '2025-05-06 14:00:00' '2025-05-06 14:10:00'");
    exit(1);
}

$startTimeStr = $argv[1];
$endTimeStr = $argv[2];

// Преобразуем строки времени в Unix timestamp
$startTime = strtotime($startTimeStr);
$endTime = strtotime($endTimeStr);

if ($startTime === false || $endTime === false) {
    $logger->error("Неверный формат времени. Используйте формат 'YYYY-MM-DD HH:MM:SS'");
    exit(1);
}

$logger->info("Повторное чтение сообщений за период с " . date('Y-m-d H:i:s', $startTime) . " по " . date('Y-m-d H:i:s', $endTime));

// Файл с картой смещений
$offsetMapFile = sys_get_temp_dir() . "/kafka_offset_time_map_{$topic}.json";

// Загружаем карту смещений
if (!file_exists($offsetMapFile)) {
    $logger->error("Файл с картой смещений не найден: {$offsetMapFile}");
    $logger->error("Сначала запустите основной консьюмер для создания карты смещений");
    exit(1);
}

$offsetTimeMap = json_decode(file_get_contents($offsetMapFile), true);
if (!$offsetTimeMap) {
    $logger->error("Не удалось загрузить карту смещений из файла");
    exit(1);
}

$logger->info("Загружена карта смещений с " . array_sum(array_map('count', $offsetTimeMap)) . " записями");

// Находим смещения, соответствующие заданному временному интервалу
$partitionOffsets = [];
foreach ($offsetTimeMap as $partition => $offsets) {
    $partitionOffsets[$partition] = [];
    
    foreach ($offsets as $offsetInfo) {
        $timestamp = $offsetInfo['timestamp'];
        
        if ($timestamp >= $startTime && $timestamp <= $endTime) {
            $partitionOffsets[$partition][] = $offsetInfo['offset'];
        }
    }
    
    // Сортируем смещения
    sort($partitionOffsets[$partition]);
}

// Проверяем, найдены ли смещения
$totalOffsets = array_sum(array_map('count', $partitionOffsets));
if ($totalOffsets === 0) {
    $logger->error("Не найдено сообщений за указанный период времени");
    exit(1);
}

$logger->info("Найдено {$totalOffsets} сообщений за указанный период времени");

// Конфигурация консьюмера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');

// Используем другую группу, чтобы не влиять на основной консьюмер
$conf->set('group.id', 'reread-time-consumer-group-' . time());

// Создаем консьюмера
$consumer = new RdKafka\Consumer($conf);

// Счетчик обработанных сообщений
$processedCount = 0;

// Обрабатываем каждую партицию отдельно
foreach ($partitionOffsets as $partition => $offsets) {
    if (empty($offsets)) {
        continue;
    }
    
    $logger->info("Чтение из партиции {$partition}, найдено " . count($offsets) . " сообщений");
    
    // Получаем топик
    $kafkaTopic = $consumer->newTopic($topic);
    
    // Для каждого смещения в партиции
    foreach ($offsets as $offset) {
        // Начинаем чтение с конкретного смещения
        $kafkaTopic->consumeStart($partition, $offset);
        
        // Читаем одно сообщение
        $message = $kafkaTopic->consume($partition, 10000);
        
        if ($message === null) {
            $logger->warning("Не удалось прочитать сообщение из партиции {$partition}, смещение {$offset}");
            continue;
        }
        
        if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            $logger->error("Ошибка при чтении сообщения: " . rd_kafka_err2str($message->err));
            continue;
        }
        
        // Декодируем JSON-сообщение
        $payload = json_decode($message->payload, true);
        
        if (!$payload) {
            $logger->error("Не удалось декодировать JSON: {$message->payload}");
            continue;
        }
        
        $processedCount++;
        
        // Обрабатываем сообщение
        $logger->info("Повторно обработано сообщение: {$payload['message']}");
        $logger->debug("Топик: {$message->topic_name}, Раздел: {$message->partition}, Смещение: {$message->offset}, Время: " . date('Y-m-d H:i:s', $payload['timestamp']));
        
        // Имитация обработки сообщения
        usleep(100000); // 100 мс
        
        // Останавливаем чтение из этой партиции
        $kafkaTopic->consumeStop($partition);
    }
}

$logger->info("Повторная обработка завершена. Обработано сообщений: {$processedCount}");
