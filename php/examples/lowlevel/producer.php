<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('lowlevel-producer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'lowlevel-topic';

// Конфигурация продюсера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');

// Создаем продюсера
$producer = new RdKafka\Producer($conf);

// Получаем топик
$kafkaTopic = $producer->newTopic($topic);

// Сообщения для отправки
$messages = [
    'Сообщение 1: ' . date('Y-m-d H:i:s'),
    'Сообщение 2: ' . date('Y-m-d H:i:s'),
    'Сообщение 3: ' . date('Y-m-d H:i:s'),
    'Сообщение 4: ' . date('Y-m-d H:i:s'),
    'Сообщение 5: ' . date('Y-m-d H:i:s'),
];

// Отправляем сообщения
foreach ($messages as $index => $message) {
    $logger->info("Отправка сообщения: {$message}");
    
    // RD_KAFKA_PARTITION_UA означает, что брокер сам выберет раздел
    $kafkaTopic->produce(RD_KAFKA_PARTITION_UA, 0, $message, "key-{$index}");
    
    // Даем время на обработку сообщения
    $producer->poll(0);
}

// Ждем, пока все сообщения будут отправлены
for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
    $result = $producer->flush(10000);
    if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
        break;
    }
}

if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
    $logger->error("Не удалось отправить все сообщения. Код ошибки: {$result}");
} else {
    $logger->info('Все сообщения успешно отправлены!');
}
