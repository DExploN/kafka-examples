package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkalib "github.com/kafka-examples/golang/src/kafka"
)

func main() {
	// Создаем логгер
	logger := log.New(os.Stdout, "advanced-consumer: ", log.LstdFlags)
	logger.Println("Запуск продвинутого консьюмера...")

	// Названия топиков, которые хотим слушать
	topics := []string{"advanced-topic", "test-topic"}

	// Создаем расширенную конфигурацию
	config := map[string]string{
		"group.id":          "advanced-consumer-group",
		"auto.offset.reset": "earliest",
		"fetch.wait.max.ms": "100",
		"fetch.error.backoff.ms": "500",
		"enable.auto.commit": "true",
		"auto.commit.interval.ms": "5000",
	}

	// Создаем консьюмера
	consumer, err := kafkalib.NewConsumer(topics, config, logger)
	if err != nil {
		logger.Fatalf("Ошибка при создании консьюмера: %v", err)
	}
	defer consumer.Close()

	// Обработчик CTRL+C для грациозного завершения
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Запускаем обработку в отдельной горутине
	go func() {
		<-sigchan
		logger.Println("Получен сигнал завершения")
		consumer.Stop()
	}()

	// Создаем обработчик сообщений с расширенной логикой
	messageHandler := func(message *kafka.Message) bool {
		logger.Printf("Обработка сообщения из топика %s: %s",
			*message.TopicPartition.Topic, string(message.Value))
		
		// Подробная информация о сообщении
		logger.Printf("  Детали: Партиция=%d, Смещение=%v, Ключ=%s",
			message.TopicPartition.Partition, message.TopicPartition.Offset,
			string(message.Key))
		
		// Имитируем обработку сообщения
		time.Sleep(100 * time.Millisecond)
		
		logger.Printf("Сообщение успешно обработано")
		return true
	}

	// Запускаем консьюмера
	consumer.Start(messageHandler)

	logger.Println("Консьюмер остановлен")
}