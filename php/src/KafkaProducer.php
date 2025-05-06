<?php

namespace App;

use Monolog\Logger;
use RdKafka\Conf;
use RdKafka\Producer;
use RdKafka\TopicConf;

class KafkaProducer
{
    private Producer $producer;
    private Logger $logger;
    private string $topicName;
    private array $config;

    public function __construct(string $topicName, array $config = [], ?Logger $logger = null)
    {
        $this->topicName = $topicName;
        $this->logger = $logger ?? new Logger('kafka-producer');
        $this->config = array_merge([
            'metadata.broker.list' => 'kafka:29092',
            'request.required.acks' => 1
        ], $config);
        
        $this->initProducer();
    }

    private function initProducer(): void
    {
        $conf = new Conf();
        
        foreach ($this->config as $key => $value) {
            $conf->set($key, $value);
        }
        
        // В новых версиях rdkafka нельзя передавать замыкания напрямую в метод set()
        $this->logger->info("Настраиваем конфигурацию Kafka продюсера");
        
        $this->producer = new Producer($conf);
    }

    public function send(string $message, ?string $key = null, int $partition = RD_KAFKA_PARTITION_UA): bool
    {
        $topic = $this->producer->newTopic($this->topicName);
        
        $this->logger->info("Отправка сообщения: {$message}");
        $topic->produce($partition, 0, $message, $key);
        $this->producer->poll(0);
        
        return true;
    }

    public function sendBatch(array $messages, ?array $keys = null): int
    {
        $topic = $this->producer->newTopic($this->topicName);
        $count = 0;
        
        foreach ($messages as $index => $message) {
            $key = $keys[$index] ?? "key-{$index}";
            $this->logger->info("Отправка сообщения #{$index}: {$message}");
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $key);
            $this->producer->poll(0);
            $count++;
        }
        
        return $count;
    }

    public function flush(int $timeout = 10000): bool
    {
        $this->logger->info("Ожидание отправки всех сообщений...");
        
        for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
            $result = $this->producer->flush($timeout);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                $this->logger->info("Все сообщения успешно отправлены!");
                return true;
            }
        }
        
        $this->logger->error("Не удалось отправить все сообщения. Код ошибки: {$result}");
        return false;
    }
}
