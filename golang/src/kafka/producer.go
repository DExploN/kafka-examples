package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer представляет Kafka продюсера
type Producer struct {
	producer *kafka.Producer
	topic    string
	logger   *log.Logger
}

// NewProducer создает новый экземпляр продюсера Kafka
func NewProducer(topic string, config map[string]string, logger *log.Logger) (*Producer, error) {
	// Создаем базовую конфигурацию
	defaultConfig := map[string]string{
		"bootstrap.servers": "kafka:29092",
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

	// Создаем продюсера
	p, err := kafka.NewProducer(&configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		producer: p,
		topic:    topic,
		logger:   logger,
	}, nil
}

// Send отправляет сообщение в Kafka
func (p *Producer) Send(value string, key string) error {
	// Создаем сообщение
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(value),
	}

	// Если ключ указан, добавляем его к сообщению
	if key != "" {
		message.Key = []byte(key)
	}

	// Отправляем сообщение
	if err := p.producer.Produce(message, nil); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	p.logger.Printf("Отправлено сообщение в топик %s: %s (ключ: %s)", p.topic, value, key)
	return nil
}

// SendBatch отправляет пакет сообщений в Kafka
func (p *Producer) SendBatch(messages []string, keys []string) (int, error) {
	sentCount := 0

	// Проверяем, что ключи совпадают с сообщениями
	if len(keys) > 0 && len(keys) != len(messages) {
		return sentCount, fmt.Errorf("количество ключей должно соответствовать количеству сообщений")
	}

	// Отправляем каждое сообщение
	for i, msg := range messages {
		key := ""
		if len(keys) > 0 {
			key = keys[i]
		}

		if err := p.Send(msg, key); err != nil {
			p.logger.Printf("Ошибка при отправке сообщения: %v", err)
			continue
		}
		sentCount++
	}

	return sentCount, nil
}

// Flush ожидает отправки всех сообщений
func (p *Producer) Flush() {
	remainingMessages := p.producer.Flush(15000) // 15 секунд
	if remainingMessages > 0 {
		p.logger.Printf("Предупреждение: %d сообщений не были отправлены", remainingMessages)
	} else {
		p.logger.Printf("Все сообщения успешно отправлены")
	}
}

// Close закрывает соединение с Kafka
func (p *Producer) Close() {
	p.producer.Close()
	p.logger.Printf("Соединение с Kafka закрыто")
}

// ProcessDeliveryReports запускает обработку отчетов о доставке сообщений
func (p *Producer) ProcessDeliveryReports(ctx context.Context) {
	go func() {
		for {
			select {
			case e := <-p.producer.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						p.logger.Printf("Ошибка доставки: %v", ev.TopicPartition.Error)
					} else {
						p.logger.Printf("Сообщение доставлено в %s [%d] со смещением %v",
							*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}