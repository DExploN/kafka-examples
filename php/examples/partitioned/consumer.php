<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('partitioned-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'partitioned-topic';

// Получаем номер партиции из аргументов командной строки или используем значение по умолчанию
$partition = isset($argv[1]) && is_numeric($argv[1]) ? (int)$argv[1] : 0;

// Конфигурация консьюмера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');

// Используем постоянный group.id для сохранения позиции между запусками
// Добавляем номер партиции, чтобы разные консьюмеры могли читать из разных партиций
$conf->set('group.id', "partitioned-consumer-group-partition-{$partition}");

// Устанавливаем auto.offset.reset в 'earliest' только для первого запуска
// Это значение будет использовано только если для группы еще нет сохраненных смещений
$conf->set('auto.offset.reset', 'earliest');

// Отключаем автоматическую фиксацию смещений, будем делать это вручную
$conf->set('enable.auto.commit', 'false');

$logger->info("Настраиваем конфигурацию Kafka для чтения из партиции {$partition} с сохранением позиции");

// Создаем KafkaConsumer для работы с группами потребителей
$consumer = new RdKafka\KafkaConsumer($conf);

// Создаем объект топик-партиция для явного назначения консьюмеру
$topicPartition = new RdKafka\TopicPartition($topic, $partition);

// Явно назначаем консьюмеру конкретную партицию вместо подписки на топик
$consumer->assign([$topicPartition]);

$logger->info("Начинаем слушать топик {$topic}, партиция {$partition} с использованием consumer group");
$logger->info("Позиция чтения будет сохранена после каждого сообщения");
$logger->info("Для выхода нажмите Ctrl+C");

// Читаем сообщения из конкретной партиции
$messageCount = 0;
$maxMessages = 100; // Ограничение на количество сообщений для чтения
$runTime = 60; // Время работы в секундах
$endTime = time() + $runTime;

while (time() < $endTime && $messageCount < $maxMessages) {
    // Читаем сообщение с таймаутом 1000 мс
    $message = $consumer->consume(1000);
    
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            // Проверяем, что сообщение из нужной партиции (дополнительная проверка)
            if ($message->partition === $partition) {
                $messageCount++;
                $logger->info("Получено сообщение: {$message->payload}");
                $logger->debug("Топик: {$message->topic_name}, Раздел: {$message->partition}, Смещение: {$message->offset}, Ключ: " . ($message->key ?: 'NULL'));
                
                // Фиксируем смещение сразу после получения сообщения
                $consumer->commit([$message]);
                $logger->debug("Смещение {$message->offset} зафиксировано");
            }
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
            $logger->error("Ошибка: {$message->errstr()}");
            break;
    }
}

// Закрываем консьюмер
$consumer->close();

$logger->info("Чтение из партиции {$partition} завершено");
$logger->info("Прочитано сообщений: {$messageCount}");
