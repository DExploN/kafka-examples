<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('retry-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'retry-topic';
$retryTopic = 'retry-topic-retry';

// Конфигурация консьюмера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');
$conf->set('group.id', 'retry-consumer-group');
$conf->set('auto.offset.reset', 'earliest');
$conf->set('enable.auto.commit', 'true');

// Создаем консьюмера
$consumer = new RdKafka\KafkaConsumer($conf);

// Подписываемся на основной топик
$consumer->subscribe([$topic]);

// Конфигурация продюсера для повторной отправки сообщений
$producerConf = new RdKafka\Conf();
$producerConf->set('metadata.broker.list', 'kafka:29092');

// Создаем продюсера для повторной отправки
$producer = new RdKafka\Producer($producerConf);
$retryKafkaTopic = $producer->newTopic($retryTopic);

$logger->info("Начинаем слушать топик {$topic}");
$logger->info("Сообщения будут обрабатываться по времени process_at");
$logger->info("Для выхода нажмите Ctrl+C");

// Максимальное количество повторных попыток
$maxRetries = 3;

// Время работы в секундах
$runTime = 300;
$endTime = time() + $runTime;

// Флаг для отслеживания работы скрипта
$running = true;

// Регистрируем обработчик сигнала прерывания (Ctrl+C)
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGINT, function () use (&$running, $logger) {
        $logger->info("Получен сигнал прерывания, завершаем работу");
        $running = false;
    });
}

// Функция для имитации обработки сообщения с возможностью ошибки
function processMessage($payload, $logger)
{
    // Имитируем случайную ошибку с вероятностью 30%
    $success = (rand(1, 100) > 30);
    
    if ($success) {
        $logger->info("Сообщение успешно обработано: {$payload['id']}");
        return true;
    } else {
        $logger->error("Ошибка при обработке сообщения: {$payload['id']}");
        return false;
    }
}

try {
    while ($running && time() < $endTime) {
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
                    
                    $logger->info("Получено сообщение: {$payload['message']}");
                    $logger->debug("ID: {$payload['id']}, Создано: " . date('Y-m-d H:i:s', $payload['created_at']) . 
                                  ", Обработать в: " . date('Y-m-d H:i:s', $payload['process_at']) . 
                                  ", Попытка: {$payload['retry_count']}");
                    
                    // Проверяем, пришло ли время обработки
                    $currentTime = time();
                    if ($payload['process_at'] > $currentTime) {
                        $waitTime = $payload['process_at'] - $currentTime;
                        $logger->info("Еще не время обработки. Ожидание {$waitTime} секунд. Перемещаем в очередь повторной обработки.");
                        
                        // Отправляем обратно в retry топик
                        $retryKafkaTopic->produce(RD_KAFKA_PARTITION_UA, 0, $message->payload, $message->key);
                        $producer->poll(0);
                        continue;
                    }
                    
                    // Пытаемся обработать сообщение
                    $success = processMessage($payload, $logger);
                    
                    if (!$success) {
                        // Если обработка не удалась, увеличиваем счетчик повторных попыток
                        $payload['retry_count']++;
                        
                        if ($payload['retry_count'] <= $maxRetries) {
                            // Вычисляем новое время обработки с экспоненциальной задержкой
                            // Базовая задержка 30 секунд, умноженная на 2^retry_count
                            $backoffTime = 30 * pow(2, $payload['retry_count'] - 1);
                            $payload['process_at'] = time() + $backoffTime;
                            
                            $logger->info("Запланирована повторная обработка через {$backoffTime} секунд (попытка {$payload['retry_count']} из {$maxRetries})");
                            
                            // Отправляем обратно в retry топик
                            $jsonPayload = json_encode($payload, JSON_UNESCAPED_UNICODE);
                            $retryKafkaTopic->produce(RD_KAFKA_PARTITION_UA, 0, $jsonPayload, $message->key);
                            $producer->poll(0);
                        } else {
                            $logger->error("Достигнуто максимальное количество попыток ({$maxRetries}) для сообщения {$payload['id']}. Сообщение будет отброшено.");
                            // Здесь можно добавить логику для сохранения неудачных сообщений в DLQ (Dead Letter Queue)
                        }
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
    // Ждем, пока все сообщения будут отправлены
    $producer->flush(10000);
    
    // Закрываем консьюмера
    $consumer->close();
    
    $logger->info("Работа завершена");
}
