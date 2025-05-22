<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use GuzzleHttp\Client;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use AvroSchema;

// Создаем логгер
$logger = new Logger('kafka-avro-second-producer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'avro-test-topic';
$subject = $topic . '-value'; // Суффикс -value для схемы значений

try {
    // Создаем клиент для Schema Registry
    $schemaRegistryClient = new Client(['base_uri' => 'http://schema-registry:8081']);

    // Создаем реестр схем
    $registry = new CachedRegistry(
        new PromisingRegistry($schemaRegistryClient),
        new AvroObjectCacheAdapter()
    );

    // Получаем последнюю версию схемы из реестра
    $logger->info("Получение последней версии схемы для субъекта: {$subject}");

    // Получаем последнюю версию схемы через REST API Schema Registry
    $response = $schemaRegistryClient->get("/subjects/{$subject}/versions/latest");
    $schemaData = json_decode($response->getBody(), true);
    $avroSchemaJson = $schemaData['schema'];

    $logger->info("Получена схема: " . $avroSchemaJson);

    // Создаем объект AvroSchema из JSON строки
    $avroSchema = AvroSchema::parse($avroSchemaJson);

    // Создаем сериализатор для Avro без опции автоматической регистрации схем
    $recordSerializer = new RecordSerializer($registry, [
        //RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => true,
        //RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => true
    ]);

    // Конфигурация продюсера
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', 'kafka:29092');

    // Создаем продюсера
    $producer = new RdKafka\Producer($conf);

    // Получаем топик
    $kafkaTopic = $producer->newTopic($topic);

    // Сообщения для отправки (должны соответствовать схеме)
    $messages = [
        [
            'id' => 101,
            'content' => 'Сообщение от second-producer: ' . date('Y-m-d H:i:s'),
            'timestamp' => time()
        ],
        [
            'id' => 102,
            'content' => 'Еще одно сообщение от second-producer: ' . date('Y-m-d H:i:s'),
            'timestamp' => time()
        ],
        [
            'id' => 103,
            'content' => 'Последнее сообщение от second-producer: ' . date('Y-m-d H:i:s'),
            'timestamp' => time()
        ]
    ];

    // Отправляем сообщения
    foreach ($messages as $index => $message) {
        $logger->info("Подготовка сообщения: " . json_encode($message));

        // Сериализуем сообщение с использованием Avro и Schema Registry
        // Используем полученную схему вместо определения её в коде
        $encodedMessage = $recordSerializer->encodeRecord(
            $subject,
            $avroSchema,
            $message
        );

        $logger->info("Отправка сообщения #{$message['id']}");

        // RD_KAFKA_PARTITION_UA означает, что брокер сам выберет раздел
        $kafkaTopic->produce(RD_KAFKA_PARTITION_UA, 0, $encodedMessage, "key-{$index}");

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
} catch (Exception $e) {
    $logger->error('Ошибка: ' . $e->getMessage());
    $logger->error($e->getTraceAsString());
}
