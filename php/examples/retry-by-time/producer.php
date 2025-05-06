<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('retry-producer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'retry-topic';

// Конфигурация продюсера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');

// Создаем продюсера
$producer = new RdKafka\Producer($conf);

// Получаем топик
$kafkaTopic = $producer->newTopic($topic);

// Генерируем сообщения с метками времени
$messages = [];
$currentTime = time();

// Создаем 10 сообщений с разными метками времени
for ($i = 0; $i < 10; $i++) {
    // Генерируем случайное время обработки (от текущего до +10 минут)
    $processTime = $currentTime + rand(0, 600);
    
    $payload = [
        'id' => uniqid(),
        'message' => "Сообщение {$i}: " . date('Y-m-d H:i:s'),
        'created_at' => $currentTime,
        'process_at' => $processTime,
        'retry_count' => 0
    ];
    
    $messages[] = $payload;
}

// Отправляем сообщения
foreach ($messages as $payload) {
    // Преобразуем данные в JSON
    $jsonPayload = json_encode($payload, JSON_UNESCAPED_UNICODE);
    
    // Используем ID сообщения как ключ
    $key = $payload['id'];
    
    $logger->info("Отправка сообщения: {$jsonPayload}");
    $logger->info("Время обработки: " . date('Y-m-d H:i:s', $payload['process_at']));
    
    // Отправляем сообщение
    $kafkaTopic->produce(RD_KAFKA_PARTITION_UA, 0, $jsonPayload, $key);
    
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
