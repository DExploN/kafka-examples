<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use GuzzleHttp\Client;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('kafka-avro-consumer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'avro-test-topic';

try {
    // Создаем клиент для Schema Registry
    $schemaRegistryClient = new Client(['base_uri' => 'http://schema-registry:8081']);

    // Создаем реестр схем
    $registry = new CachedRegistry(
        new PromisingRegistry($schemaRegistryClient),
        new AvroObjectCacheAdapter()
    );

    // Создаем десериализатор для Avro с опцией автоматической регистрации схем
    $recordSerializer = new RecordSerializer($registry, [
        RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => true,
        RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => true
    ]);

    // Конфигурация консьюмера
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', 'kafka:29092');
    $conf->set('group.id', 'php-avro-consumer-group');
    $conf->set('auto.offset.reset', 'earliest');

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
                try {
                    // Десериализуем сообщение с использованием Avro и Schema Registry
                    $decodedMessage = $recordSerializer->decodeMessage($message->payload);

                    $logger->info("Получено сообщение:");
                    $logger->info("ID: {$decodedMessage['id']}");
                    $logger->info("Содержимое: {$decodedMessage['content']}");
                    $logger->info("Временная метка: " . date('Y-m-d H:i:s', $decodedMessage['timestamp']));

                    // Проверяем, является ли заголовок null
                    $titleValue = $decodedMessage['title'] === null ? 'NULL (заголовок отсутствует)' : $decodedMessage['title'];
                    $logger->info("Заголовок: {$titleValue}");

                    // Проверяем, есть ли поле work в сообщении (для обратной совместимости)
                    if (isset($decodedMessage['work'])) {
                        $logger->info("Работа: {$decodedMessage['work']}");
                        $logger->info("Версия схемы: Новая (с полем 'work')");
                    } else {
                        $logger->info("Версия схемы: Старая (без поля 'work')");
                    }

                    $logger->debug("Топик: {$message->topic_name}, Раздел: {$message->partition}, Смещение: {$message->offset}, Ключ: " . ($message->key ?: 'NULL'));
                } catch (Exception $e) {
                    $logger->error("Ошибка десериализации: " . $e->getMessage());
                }
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
} catch (Exception $e) {
    $logger->error('Ошибка: ' . $e->getMessage());
    $logger->error($e->getTraceAsString());
}
