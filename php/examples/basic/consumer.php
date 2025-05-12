<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('kafka-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'test-topic';

// Конфигурация консьюмера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');
$conf->set('group.id', 'php-consumer-group');
$conf->set('auto.offset.reset', 'earliest');

// В новых версиях rdkafka нельзя передавать замыкания напрямую в метод set()
$logger->info("Настраиваем конфигурацию Kafka");

// Создаем консьюмера
$consumer = new RdKafka\KafkaConsumer($conf);

// Подписываемся на топик
$consumer->subscribe([$topic]);

$logger->info("Начинаем слушать топик {$topic}");
$logger->info("Для выхода нажмите Ctrl+C");



// Читаем сообщения
while (true) {
    $message = $consumer->consume(10000);
    
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $logger->info("Получено сообщение: {$message->payload}");
            $logger->debug("Топик: {$message->topic_name}, Раздел: {$message->partition}, Смещение: {$message->offset}, Ключ: " . ($message->key ?: 'NULL'));
            break;
            
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            $logger->info("Достигнут конец раздела");
            break;
            
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            $logger->debug("Тайм-аут, продолжаем...");
            break;
        default:
            $logger->error("Ошибка: {$message->errstr()}");
            break;

    }
}
