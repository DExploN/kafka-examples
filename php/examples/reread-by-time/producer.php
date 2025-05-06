<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('reread-producer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'reread-topic';

// Конфигурация продюсера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');

// Создаем продюсера
$producer = new RdKafka\Producer($conf);

// Получаем топик
$kafkaTopic = $producer->newTopic($topic);

// Количество сообщений для генерации
$messageCount = isset($argv[1]) ? (int)$argv[1] : 20;

// Генерируем сообщения с метками времени
$messages = [];
$startTime = time() - $messageCount; // Начинаем с прошлого времени для демонстрации

$logger->info("Генерируем {$messageCount} сообщений с метками времени");

for ($i = 0; $i < $messageCount; $i++) {
    // Создаем сообщение с временной меткой
    $timestamp = $startTime + $i;
    
    $payload = [
        'id' => uniqid(),
        'message' => "Сообщение {$i}: событие в " . date('Y-m-d H:i:s', $timestamp),
        'timestamp' => $timestamp,
        'value' => rand(1, 100)
    ];
    
    $messages[] = $payload;
}

// Отправляем сообщения
foreach ($messages as $payload) {
    // Преобразуем данные в JSON
    $jsonPayload = json_encode($payload, JSON_UNESCAPED_UNICODE);
    
    // Используем ID сообщения как ключ
    $key = $payload['id'];
    
    $logger->info("Отправка сообщения: {$payload['message']}");
    $logger->debug("Время события: " . date('Y-m-d H:i:s', $payload['timestamp']));
    
    // Отправляем сообщение
    // Используем timestamp сообщения как Kafka timestamp (в миллисекундах)
    $kafkaTopic->producev(
        RD_KAFKA_PARTITION_UA, // Автоматический выбор партиции
        0,                     // Флаги
        $jsonPayload,          // Сообщение
        $key,                  // Ключ
        null,                  // Заголовки (null = без заголовков)
        $payload['timestamp'] * 1000 // Timestamp в миллисекундах
    );
    
    // Даем время на обработку сообщения
    $producer->poll(0);
    
    // Небольшая задержка для демонстрации
    usleep(100000); // 100 мс
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
