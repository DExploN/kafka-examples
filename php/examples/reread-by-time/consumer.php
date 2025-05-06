<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('reread-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'reread-topic';

// Конфигурация консьюмера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');
$conf->set('group.id', 'reread-consumer-group');
$conf->set('auto.offset.reset', 'earliest');
$conf->set('enable.auto.commit', 'true');

// Создаем консьюмера
$consumer = new RdKafka\KafkaConsumer($conf);

// Подписываемся на топик
$consumer->subscribe([$topic]);

$logger->info("Начинаем слушать топик {$topic}");
$logger->info("Для выхода нажмите Ctrl+C");

// Файл для сохранения информации о сообщениях
$offsetMapFile = sys_get_temp_dir() . "/kafka_offset_time_map_{$topic}.json";

// Загружаем существующую карту смещений, если она есть
$offsetTimeMap = [];
if (file_exists($offsetMapFile)) {
    $offsetTimeMap = json_decode(file_get_contents($offsetMapFile), true) ?: [];
    $logger->info("Загружена карта смещений с " . count($offsetTimeMap) . " записями");
}

// Флаг для отслеживания работы скрипта
$running = true;

// Регистрируем обработчик сигнала прерывания (Ctrl+C)
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGINT, function () use (&$running, $logger, $offsetMapFile, &$offsetTimeMap) {
        $logger->info("Получен сигнал прерывания, сохраняем карту смещений и завершаем работу");
        file_put_contents($offsetMapFile, json_encode($offsetTimeMap, JSON_PRETTY_PRINT));
        $running = false;
    });
}

// Счетчик сообщений
$messageCount = 0;

try {
    while ($running) {
        // Читаем сообщение с таймаутом 1000 мс
        $message = $consumer->consume(1000);
        
        if ($message === null) {
            continue;
        }
        
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                try {
                    // Декодируем JSON-сообщение
                    $payload = json_decode($message->payload, true);
                    
                    if (!$payload) {
                        $logger->error("Не удалось декодировать JSON: {$message->payload}");
                        continue;
                    }
                    
                    $messageCount++;
                    
                    // Получаем информацию о партиции и смещении
                    $partition = $message->partition;
                    $offset = $message->offset;
                    $timestamp = $payload['timestamp'];
                    
                    // Сохраняем информацию о времени сообщения и его смещении
                    if (!isset($offsetTimeMap[$partition])) {
                        $offsetTimeMap[$partition] = [];
                    }
                    $offsetTimeMap[$partition][] = [
                        'offset' => $offset,
                        'timestamp' => $timestamp,
                        'datetime' => date('Y-m-d H:i:s', $timestamp)
                    ];
                    
                    // Обрабатываем сообщение
                    $logger->info("Получено сообщение #{$messageCount}: {$payload['message']}");
                    $logger->debug("Топик: {$message->topic_name}, Раздел: {$partition}, Смещение: {$offset}, Время: " . date('Y-m-d H:i:s', $timestamp));
                    
                    // Имитация обработки сообщения
                    usleep(100000); // 100 мс
                    
                    // Периодически сохраняем карту смещений
                    if ($messageCount % 10 === 0) {
                        file_put_contents($offsetMapFile, json_encode($offsetTimeMap, JSON_PRETTY_PRINT));
                        $logger->info("Карта смещений сохранена. Всего записей: " . array_sum(array_map('count', $offsetTimeMap)));
                    }
                } catch (Exception $e) {
                    $logger->error("Ошибка при обработке сообщения: " . $e->getMessage());
                }
                break;
                
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                $logger->info("Достигнут конец раздела");
                break;
                
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $logger->debug("Тайм-аут, продолжаем...");
                break;
                
            default:
                $logger->error("Ошибка: " . rd_kafka_err2str($message->err));
                break;
        }
    }
} finally {
    // Сохраняем карту смещений перед выходом
    file_put_contents($offsetMapFile, json_encode($offsetTimeMap, JSON_PRETTY_PRINT));
    $logger->info("Карта смещений сохранена. Всего записей: " . array_sum(array_map('count', $offsetTimeMap)));
    
    // Закрываем консьюмера
    $consumer->close();
    
    $logger->info("Работа завершена. Обработано сообщений: {$messageCount}");
}
