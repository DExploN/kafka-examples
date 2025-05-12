package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// getPartition вычисляет партицию на основе ключа
func getPartition(key string, partitionCount int) int {
	// Используем crc32 для получения хеша ключа
	hash := crc32.ChecksumIEEE([]byte(key))
	// Преобразуем хеш в положительное число и получаем остаток от деления на количество партиций
	return int(hash) % partitionCount
}

func main() {
	// Создаем логгер
	logger := log.New(os.Stdout, "partitioned-producer: ", log.LstdFlags)
	logger.Println("Запуск продюсера с определенными партициями...")

	// Название топика и количество партиций
	topic := "partitioned-topic-go"
	partitionCount := 3

	// Создаем конфигурацию
	config := kafka.ConfigMap{
		"bootstrap.servers": "kafka:29092",
	}

	// Создаем продюсера
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		logger.Fatalf("Ошибка при создании продюсера: %v", err)
	}
	defer producer.Close()

	// Запускаем обработку отчетов о доставке
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case e := <-producer.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						logger.Printf("Ошибка доставки: %v", ev.TopicPartition.Error)
					} else {
						logger.Printf("Сообщение доставлено в %s [%d] со смещением %v",
							*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Список ключей для отправки
	keys := []string{
		"user:1", "user:2", "user:3", "user:4", "user:5",
		"order:1", "order:2", "order:3",
		"product:1", "product:2",
	}

	// Отправляем сообщения с разными ключами
	for _, key := range keys {
		// Вычисляем партицию на основе ключа
		partition := getPartition(key, partitionCount)
		
		// Создаем сообщение с указанием партиции
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: int32(partition),
			},
			Key:   []byte(key),
			Value: []byte(fmt.Sprintf("Сообщение с ключом %s (время: %s)", key, time.Now().Format(time.RFC3339))),
		}

		logger.Printf("Отправка сообщения с ключом %s в партицию %d", key, partition)
		
		// Отправляем сообщение
		if err := producer.Produce(message, nil); err != nil {
			logger.Printf("Ошибка при отправке сообщения: %v", err)
		}

		// Делаем небольшую паузу между сообщениями
		time.Sleep(500 * time.Millisecond)
	}

	// Ждем, пока все сообщения будут отправлены
	remaining := producer.Flush(10000)
	if remaining > 0 {
		logger.Printf("Предупреждение: %d сообщений не были отправлены", remaining)
	}

	logger.Println("Все сообщения отправлены!")
}