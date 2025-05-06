<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('lowlevel-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'lowlevel-topic';

// Получаем номер партиции из аргументов командной строки или используем значение по умолчанию
$partition = isset($argv[1]) && is_numeric($argv[1]) ? (int)$argv[1] : 0;

// Конфигурация консьюмера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');

// Создаем консьюмера используя низкоуровневый API
$consumer = new RdKafka\Consumer($conf);

// Получаем топик
$kafkaTopic = $consumer->newTopic($topic);

// Начинаем читать сообщения из указанной партиции с начала
// RD_KAFKA_OFFSET_BEGINNING - начать с начала партиции
// RD_KAFKA_OFFSET_END - начать с конца партиции (только новые сообщения)
// RD_KAFKA_OFFSET_STORED - начать с сохраненного смещения (не работает с низкоуровневым API)
// Также можно указать конкретное смещение (число)
$kafkaTopic->consumeStart($partition, RD_KAFKA_OFFSET_BEGINNING);

$logger->info("Начинаем слушать топик {$topic}, партиция {$partition} с использованием низкоуровневого API");
$logger->info("Для выхода нажмите Ctrl+C");

// Создаем файл для сохранения смещения
$offsetFile = sys_get_temp_dir() . "/kafka_offset_{$topic}_{$partition}.txt";

// Загружаем сохраненное смещение, если оно есть
if (file_exists($offsetFile)) {
    $savedOffset = (int)file_get_contents($offsetFile);
    if ($savedOffset > 0) {
        $logger->info("Найдено сохраненное смещение: {$savedOffset}, перезапускаем консьюмер с этой позиции");
        $kafkaTopic->consumeStop($partition);
        $kafkaTopic->consumeStart($partition, $savedOffset);
    }
}

// Читаем сообщения из конкретной партиции
$messageCount = 0;
$maxMessages = 100; // Ограничение на количество сообщений для чтения
$runTime = 60; // Время работы в секундах
$endTime = time() + $runTime;
$lastOffset = 0;

// Регистрируем обработчик сигнала прерывания (Ctrl+C)
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGINT, function () use (&$running, $logger, $offsetFile, &$lastOffset) {
        $logger->info("Получен сигнал прерывания, сохраняем последнее смещение {$lastOffset} и завершаем работу");
        if ($lastOffset > 0) {
            file_put_contents($offsetFile, $lastOffset);
        }
        exit(0);
    });
}

try {
    while (time() < $endTime && $messageCount < $maxMessages) {
        // Читаем сообщение с таймаутом 1000 мс
        $message = $kafkaTopic->consume($partition, 1000);
        
        if ($message === null) {
            continue;
        }
        
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $messageCount++;
                $lastOffset = $message->offset + 1; // Сохраняем следующее смещение
                
                $logger->info("Получено сообщение: {$message->payload}");
                $logger->debug("Топик: {$message->topic_name}, Раздел: {$message->partition}, Смещение: {$message->offset}, Ключ: " . ($message->key ?: 'NULL'));
                
                // Сохраняем смещение в файл после каждого сообщения
                file_put_contents($offsetFile, $lastOffset);
                $logger->debug("Смещение {$lastOffset} сохранено в файл");
                break;
                
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                $logger->info("Достигнут конец раздела {$partition}");
                // Ждем немного, чтобы не загружать CPU
                sleep(1);
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
    // Останавливаем чтение из партиции
    $kafkaTopic->consumeStop($partition);
    
    // Сохраняем последнее смещение
    if ($lastOffset > 0) {
        file_put_contents($offsetFile, $lastOffset);
        $logger->info("Последнее смещение {$lastOffset} сохранено в файл {$offsetFile}");
    }
    
    $logger->info("Чтение из партиции {$partition} завершено");
    $logger->info("Прочитано сообщений: {$messageCount}");
}
