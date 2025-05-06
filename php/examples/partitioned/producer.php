<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('partitioned-producer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'partitioned-topic';

// Количество партиций в топике (должно соответствовать настройкам топика)
$partitionCount = 3;

// Конфигурация продюсера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');

// Создаем продюсера
$producer = new RdKafka\Producer($conf);

// Получаем топик
$kafkaTopic = $producer->newTopic($topic);

// Функция для определения партиции на основе ключа
function getPartition($key, $partitionCount) {
    // Используем crc32 для получения хеша ключа
    $hash = crc32($key);
    // Преобразуем хеш в положительное число и получаем остаток от деления на количество партиций
    return abs($hash) % $partitionCount;
}

// Отправляем сообщения в конкретные партиции
$messages = [
    'user-1' => 'Сообщение для пользователя 1: ' . date('Y-m-d H:i:s'),
    'user-2' => 'Сообщение для пользователя 2: ' . date('Y-m-d H:i:s'),
    'user-3' => 'Сообщение для пользователя 3: ' . date('Y-m-d H:i:s'),
    'user-4' => 'Сообщение для пользователя 4: ' . date('Y-m-d H:i:s'),
    'user-5' => 'Сообщение для пользователя 5: ' . date('Y-m-d H:i:s'),
];

// Отправляем сообщения с явным указанием партиции
foreach ($messages as $key => $message) {
    // Определяем партицию на основе ключа
    $partition = getPartition($key, $partitionCount);
    
    $logger->info("Отправка сообщения: {$message} с ключом {$key} в партицию {$partition}");
    
    // Отправляем сообщение в конкретную партицию
    $kafkaTopic->produce($partition, 0, $message, $key);
    
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

// Выводим информацию о распределении сообщений по партициям
$logger->info('Распределение сообщений по партициям:');
foreach ($messages as $key => $message) {
    $partition = getPartition($key, $partitionCount);
    $logger->info("Ключ: {$key} -> Партиция: {$partition}");
}
