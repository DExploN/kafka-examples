package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// MessageHandler - тип функции для обработки сообщений
type MessageHandler func(*kafka.Message) bool

// Consumer представляет Kafka консьюмера
type Consumer struct {
	consumer *kafka.Consumer
	topics   []string
	logger   *log.Logger
	running  bool
}

// NewConsumer создает новый экземпляр консьюмера Kafka
func NewConsumer(topics []string, config map[string]string, logger *log.Logger) (*Consumer, error) {
	// Создаем базовую конфигурацию
	defaultConfig := map[string]string{
		"bootstrap.servers":  "kafka:29092",
		"group.id":           "go-consumer-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	}

	// Объединяем с пользовательской конфигурацией
	for k, v := range config {
		defaultConfig[k] = v
	}

	// Преобразуем map в kafka.ConfigMap
	configMap := kafka.ConfigMap{}
	for k, v := range defaultConfig {
		configMap[k] = v
	}

	// Создаем консьюмера
	c, err := kafka.NewConsumer(&configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	// Подписываемся на топики
	if err := c.SubscribeTopics(topics, nil); err != nil {
		c.Close()
		return nil, fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	return &Consumer{
		consumer: c,
		topics:   topics,
		logger:   logger,
		running:  false,
	}, nil
}

// Consume получает сообщение из Kafka с таймаутом
func (c *Consumer) Consume(timeoutMs int, handler MessageHandler) error {
	// Получаем сообщение с указанным таймаутом
	msg, err := c.consumer.ReadMessage(time.Duration(timeoutMs) * time.Millisecond)
	if err != nil {
		// Проверяем, является ли ошибка таймаутом
		if err.(kafka.Error).Code() == kafka.ErrTimedOut {
			return nil // Таймаут - не ошибка
		}
		return fmt.Errorf("error consuming message: %w", err)
	}

	// Логируем полученное сообщение
	c.logger.Printf("Получено сообщение из топика %s [%d] со смещением %v: %s",
		*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))

	// Обрабатываем сообщение, если предоставлен обработчик
	if handler != nil {
		continueProcessing := handler(msg)
		if !continueProcessing {
			c.running = false
		}
	}

	return nil
}

// Start запускает консьюмера в бесконечном цикле
func (c *Consumer) Start(handler MessageHandler) {
	c.running = true
	c.logger.Printf("Начинаем слушать топики: %v", c.topics)
	c.logger.Printf("Для выхода нажмите Ctrl+C")

	for c.running {
		if err := c.Consume(100, handler); err != nil {
			c.logger.Printf("Ошибка при потреблении сообщения: %v", err)
		}
	}
}

// Stop останавливает консьюмера
func (c *Consumer) Stop() {
	c.running = false
	c.logger.Printf("Остановка консьюмера")
}

// Close закрывает соединение с Kafka
func (c *Consumer) Close() {
	c.consumer.Close()
	c.logger.Printf("Соединение с Kafka закрыто")
}

// StartWithTimeout запускает консьюмера на указанное время
func (c *Consumer) StartWithTimeout(handler MessageHandler, timeoutSeconds int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	c.running = true
	c.logger.Printf("Начинаем слушать топики с таймаутом %d секунд: %v", timeoutSeconds, c.topics)

	go func() {
		<-ctx.Done()
		c.Stop()
	}()

	for c.running {
		if err := c.Consume(100, handler); err != nil {
			c.logger.Printf("Ошибка при потреблении сообщения: %v", err)
		}
	}
}