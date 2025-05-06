<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use App\KafkaConsumer;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('advanced-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Названия топиков, которые хотим слушать
$topics = ['advanced-topic', 'test-topic'];

// Создаем консьюмера
$consumer = new KafkaConsumer($topics, [
    'group.id' => 'advanced-consumer-group'
], $logger);

// Обработчик сообщений
$messageHandler = function (\RdKafka\Message $message) use ($logger) {
    $logger->info("Обработка сообщения: {$message->payload}");
    
    // Здесь можно добавить логику обработки сообщения
    // Например, декодирование JSON, сохранение в базу данных и т.д.
    
    // Имитация обработки
    usleep(100000); // 100 мс
    
    $logger->info("Сообщение успешно обработано");
};

// Запускаем консьюмер в бесконечном цикле
$consumer->start($messageHandler);

// Этот код не будет выполнен, пока консьюмер не остановится (Ctrl+C)
$logger->info('Работа консьюмера завершена');
