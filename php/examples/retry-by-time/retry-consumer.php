<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('retry-topic-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика для повторной обработки
$retryTopic = 'retry-topic-retry';
$mainTopic = 'retry-topic';

// Конфигурация консьюмера
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'kafka:29092');
$conf->set('group.id', 'retry-topic-consumer-group');
$conf->set('auto.offset.reset', 'earliest');
$conf->set('enable.auto.commit', 'true');

// Создаем консьюмера
$consumer = new RdKafka\KafkaConsumer($conf);

// Подписываемся на топик повторной обработки
$consumer->subscribe([$retryTopic]);

// Конфигурация продюсера для перемещения сообщений в основной топик
$producerConf = new RdKafka\Conf();
$producerConf->set('metadata.broker.list', 'kafka:29092');

// Создаем продюсера
$producer = new RdKafka\Producer($producerConf);
$mainKafkaTopic = $producer->newTopic($mainTopic);

$logger->info("Начинаем слушать топик повторной обработки {$retryTopic}");
$logger->info("Сообщения будут перемещены в основной топик {$mainTopic}, когда наступит время их обработки");
$logger->info("Для выхода нажмите Ctrl+C");

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
                    
                    $logger->info("Получено сообщение из очереди повторной обработки: {$payload['message']}");
                    $logger->debug("ID: {$payload['id']}, Создано: " . date('Y-m-d H:i:s', $payload['created_at']) . 
                                  ", Обработать в: " . date('Y-m-d H:i:s', $payload['process_at']) . 
                                  ", Попытка: {$payload['retry_count']}");
                    
                    // Проверяем, пришло ли время обработки
                    $currentTime = time();
                    if ($payload['process_at'] <= $currentTime) {
                        $logger->info("Время обработки наступило. Перемещаем сообщение в основной топик.");
                        
                        // Отправляем в основной топик для обработки
                        $mainKafkaTopic->produce(RD_KAFKA_PARTITION_UA, 0, $message->payload, $message->key);
                        $producer->poll(0);
                    } else {
                        $waitTime = $payload['process_at'] - $currentTime;
                        $logger->info("Еще не время обработки. Ожидание {$waitTime} секунд. Оставляем в очереди повторной обработки.");
                        
                        // Отправляем обратно в retry топик
                        $retryKafkaTopic = $producer->newTopic($retryTopic);
                        $retryKafkaTopic->produce(RD_KAFKA_PARTITION_UA, 0, $message->payload, $message->key);
                        $producer->poll(0);
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
