<?php

namespace App;

use Monolog\Logger;
use RdKafka\Conf;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message;

class KafkaConsumer
{
    private RdKafkaConsumer $consumer;
    private Logger $logger;
    private array $topics;
    private array $config;
    private bool $running = false;

    public function __construct(array $topics, array $config = [], ?Logger $logger = null)
    {
        $this->topics = $topics;
        $this->logger = $logger ?? new Logger('kafka-consumer');
        $this->config = array_merge([
            'metadata.broker.list' => 'kafka:29092',
            'group.id' => 'php-consumer-group',
            'auto.offset.reset' => 'earliest',
            'enable.auto.commit' => 'true',
            'auto.commit.interval.ms' => '1000'
        ], $config);
        
        $this->initConsumer();
    }

    private function initConsumer(): void
    {
        $conf = new Conf();
        
        foreach ($this->config as $key => $value) {
            $conf->set($key, $value);
        }
        
        // В новых версиях rdkafka нельзя передавать замыкания напрямую в метод set()
        // Для обработки ошибок и ребалансировки мы будем использовать обработку в методе consume
        $this->logger->info("Настраиваем конфигурацию Kafka");
        
        $this->consumer = new RdKafkaConsumer($conf);
        $this->consumer->subscribe($this->topics);
        
        $this->logger->info("Подписка на топики: " . implode(', ', $this->topics));
    }

    public function consume(int $timeout = 10000, callable $messageHandler = null): ?Message
    {
        $message = $this->consumer->consume($timeout);
        
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->logger->info("Получено сообщение: {$message->payload}");
                $this->logger->debug("Топик: {$message->topic_name}, Раздел: {$message->partition}, Смещение: {$message->offset}, Ключ: " . ($message->key ?: 'NULL'));
                
                if ($messageHandler) {
                    call_user_func($messageHandler, $message);
                }
                
                return $message;
                
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                $this->logger->debug("Достигнут конец раздела");
                break;
                
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->logger->debug("Тайм-аут, продолжаем...");
                break;
                
            default:
                $this->logger->error("Ошибка: {$message->errstr()}");
                break;
        }
        
        return null;
    }

    public function start(callable $messageHandler, int $timeout = 10000): void
    {
        $this->running = true;
        $this->logger->info("Начинаем слушать топики: " . implode(', ', $this->topics));
        $this->logger->info("Для выхода нажмите Ctrl+C");
        
        while ($this->running) {
            $this->consume($timeout, $messageHandler);
        }
    }

    public function stop(): void
    {
        $this->running = false;
        $this->logger->info("Остановка консьюмера");
    }
}
