package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkalib "github.com/kafka-examples/golang/src/kafka"
)

func main() {
	// Создаем логгер
	logger := log.New(os.Stdout, "basic-consumer: ", log.LstdFlags)
	logger.Println("Запуск консьюмера...")

	// Название топика
	topics := []string{"basic-topic"}

	// Создаем конфигурацию
	config := map[string]string{
		"group.id":          "basic-consumer-group",
		"auto.offset.reset": "earliest", // Начинаем с самого раннего сообщения
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

	// Создаем обработчик сообщений
	messageHandler := func(message *kafka.Message) bool {
		logger.Printf("Получено сообщение: %s", string(message.Value))

		// Для демонстрации, можем проверить содержимое сообщения
		// и остановить обработку по определенному условию
		// if strings.Contains(string(message.Value), "stop") {
		//     return false  // остановить обработку
		// }
		
		return true // продолжать обработку
	}

	// Запускаем консьюмера
	consumer.Start(messageHandler)

	logger.Println("Консьюмер остановлен")
}