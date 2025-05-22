package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
)

// setupConsumer создает и настраивает Kafka консьюмера
func setupConsumer(topics []string, groupID string, logger *log.Logger) (*kafka.Consumer, error) {
	// Конфигурация консьюмера
	config := &kafka.ConfigMap{
		"bootstrap.servers":  "kafka:29092",
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	}

	// Создаем консьюмера
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("ошибка при создании консьюмера: %w", err)
	}

	// Подписываемся на топики
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("ошибка при подписке на топики: %w", err)
	}

	return consumer, nil
}

// deserializeAvroMessage десериализует Avro сообщение с использованием Schema Registry
func deserializeAvroMessage(
	msg []byte, 
	schemaRegistryClient *srclient.SchemaRegistryClient, 
	schemaCache map[int]*goavro.Codec,
	logger *log.Logger,
) (map[string]interface{}, error) {
	// Проверяем, что сообщение имеет правильный формат
	if len(msg) <= 5 {
		return nil, fmt.Errorf("сообщение неверного формата")
	}

	// Извлекаем Magic Byte и Schema ID
	magicByte := msg[0]
	schemaID := int(binary.BigEndian.Uint32(msg[1:5]))
	avroData := msg[5:]

	if magicByte != 0 {
		return nil, fmt.Errorf("неверный Magic Byte: %d", magicByte)
	}

	// Получаем схему из кэша или из Schema Registry
	codec, ok := schemaCache[schemaID]
	if !ok {
		schema, err := schemaRegistryClient.GetSchema(schemaID)
		if err != nil {
			return nil, fmt.Errorf("ошибка при получении схемы: %w", err)
		}

		codec, err = goavro.NewCodec(schema.Schema())
		if err != nil {
			return nil, fmt.Errorf("ошибка при создании Avro кодека: %w", err)
		}

		// Кэшируем кодек
		schemaCache[schemaID] = codec
	}

	// Десериализуем сообщение
	native, _, err := codec.NativeFromBinary(avroData)
	if err != nil {
		return nil, fmt.Errorf("ошибка при десериализации сообщения: %w", err)
	}

	// Преобразуем в map для удобного доступа к полям
	record, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("ошибка при приведении типа сообщения")
	}

	return record, nil
}

// logMessageInfo выводит информацию о десериализованном сообщении
func logMessageInfo(record map[string]interface{}, msg *kafka.Message, logger *log.Logger) {
	logger.Println("Получено сообщение:")
	logger.Printf("ID: %v", record["id"])
	logger.Printf("Содержимое: %v", record["content"])

	// Форматируем временную метку
	timestamp, ok := record["timestamp"].(int64)
	if ok {
		logger.Printf("Временная метка: %s", time.Unix(timestamp, 0).Format(time.RFC3339))
	} else {
		logger.Printf("Временная метка: %v", record["timestamp"])
	}

	// Проверяем, является ли заголовок null
	title, hasTitle := record["title"]
	if !hasTitle || title == nil {
		logger.Printf("Заголовок: NULL (заголовок отсутствует)")
	} else {
		logger.Printf("Заголовок: %v", title)
	}

	logger.Printf("Топик: %s, Раздел: %d, Смещение: %v, Ключ: %s",
		*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key))
}

func main() {
	// Создаем логгер
	logger := log.New(os.Stdout, "avro-consumer: ", log.LstdFlags)
	logger.Println("Запуск консьюмера с Avro и Schema Registry...")

	// Название топика
	topic := "avro-test-topic"

	// Создаем клиент для Schema Registry
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://schema-registry:8081")
	logger.Println("Подключение к Schema Registry...")

	// Создаем и настраиваем консьюмера
	consumer, err := setupConsumer([]string{topic}, "go-avro-consumer-group", logger)
	if err != nil {
		logger.Fatalf("Ошибка при настройке консьюмера: %v", err)
	}
	defer consumer.Close()

	// Обработчик CTRL+C для грациозного завершения
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Флаг для контроля цикла обработки сообщений
	run := true

	// Запускаем обработку в отдельной горутине
	go func() {
		<-sigchan
		logger.Println("Получен сигнал завершения")
		run = false
	}()

	logger.Printf("Начинаем слушать топик %s", topic)
	logger.Println("Для выхода нажмите Ctrl+C")

	// Кэш для схем
	schemaCache := make(map[int]*goavro.Codec)

	// Читаем сообщения
	for run {
		msg, err := consumer.ReadMessage(100 * time.Millisecond)
		if err != nil {
			// Проверяем, является ли ошибка таймаутом
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue // Таймаут - не ошибка
			}
			logger.Printf("Ошибка при чтении сообщения: %v", err)
			continue
		}

		// Десериализуем сообщение
		record, err := deserializeAvroMessage(msg.Value, schemaRegistryClient, schemaCache, logger)
		if err != nil {
			logger.Printf("Ошибка при обработке сообщения: %v", err)
			continue
		}

		// Выводим информацию о сообщении
		logMessageInfo(record, msg, logger)
	}

	logger.Println("Консьюмер остановлен")
}
