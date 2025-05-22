package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

// serializeAvroWithSchemaID сериализует сообщение с использованием Avro и добавляет Schema ID
func serializeAvroWithSchemaID(codec *goavro.Codec, schemaID int, message map[string]interface{}) ([]byte, error) {
	// Сериализуем сообщение с использованием Avro
	binaryData, err := codec.BinaryFromNative(nil, message)
	if err != nil {
		return nil, fmt.Errorf("ошибка при сериализации сообщения: %w", err)
	}

	// Добавляем Magic Byte и Schema ID в начало сообщения
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))

	// Формат сообщения: Magic Byte (1 байт) + Schema ID (4 байта) + Avro данные
	payload := append([]byte{0}, schemaIDBytes...)
	payload = append(payload, binaryData...)

	return payload, nil
}

// setupProducer создает и настраивает Kafka продюсера
func setupProducer(logger *log.Logger) (*kafka.Producer, error) {
	// Конфигурация продюсера
	config := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:29092",
	}

	// Создаем продюсера
	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании продюсера: %w", err)
	}

	// Обработка отчетов о доставке в отдельной горутине
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logger.Printf("Ошибка доставки: %v", ev.TopicPartition.Error)
				} else {
					logger.Printf("Сообщение доставлено в %s [%d] со смещением %v",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	return producer, nil
}

func main() {
	// Создаем логгер
	logger := log.New(os.Stdout, "avro-second-producer: ", log.LstdFlags)
	logger.Println("Запуск второго продюсера с Avro и Schema Registry...")

	// Название топика
	topic := "avro-test-topic"
	subject := topic + "-value" // Суффикс -value для схемы значений

	// Создаем клиент для Schema Registry
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://schema-registry:8081")
	logger.Println("Подключение к Schema Registry...")

	// Получаем последнюю версию схемы из реестра
	logger.Printf("Получение последней версии схемы для субъекта: %s", subject)

	schema, err := schemaRegistryClient.GetLatestSchema(subject)
	if err != nil {
		logger.Fatalf("Ошибка при получении схемы: %v", err)
	}

	logger.Printf("Получена схема с ID: %d", schema.ID())
	logger.Printf("Схема: %s", schema.Schema())

	// Создаем кодек для сериализации Avro
	codec, err := goavro.NewCodec(schema.Schema())
	if err != nil {
		logger.Fatalf("Ошибка при создании Avro кодека: %v", err)
	}

	// Создаем и настраиваем продюсера
	producer, err := setupProducer(logger)
	if err != nil {
		logger.Fatalf("Ошибка при настройке продюсера: %v", err)
	}
	defer producer.Close()

	// Сообщения для отправки
	messages := []map[string]interface{}{
		{
			"id":        101,
			"content":   fmt.Sprintf("Сообщение от second-producer: %s", time.Now().Format(time.RFC3339)),
			"timestamp": time.Now().Unix(),
			"title":     nil, // Первое сообщение без заголовка
		},
		{
			"id":        102,
			"content":   fmt.Sprintf("Еще одно сообщение от second-producer: %s", time.Now().Format(time.RFC3339)),
			"timestamp": time.Now().Unix(),
			"title":     "Заголовок от second-producer",
		},
		{
			"id":        103,
			"content":   fmt.Sprintf("Последнее сообщение от second-producer: %s", time.Now().Format(time.RFC3339)),
			"timestamp": time.Now().Unix(),
			"title":     nil, // Последнее сообщение тоже без заголовка
		},
	}

	// Отправляем сообщения
	for i, msg := range messages {
		logger.Printf("Подготовка сообщения: %v", msg)

		// Сериализуем сообщение с использованием Avro и добавляем Schema ID
		payload, err := serializeAvroWithSchemaID(codec, schema.ID(), msg)
		if err != nil {
			logger.Printf("Ошибка при сериализации сообщения: %v", err)
			continue
		}

		// Отправляем сообщение
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: payload,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
		}, nil)

		if err != nil {
			logger.Printf("Ошибка при отправке сообщения: %v", err)
			continue
		}

		logger.Printf("Отправка сообщения #%d", i+1)

		// Даем время на обработку сообщения
		time.Sleep(500 * time.Millisecond)
	}

	// Ждем, пока все сообщения будут отправлены
	remaining := producer.Flush(15000)
	if remaining > 0 {
		logger.Printf("Предупреждение: %d сообщений не были отправлены", remaining)
	} else {
		logger.Println("Все сообщения успешно отправлены!")
	}
}
