package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kafka-examples/golang/src/kafka"
)

func main() {
	// Создаем логгер
	logger := log.New(os.Stdout, "basic-producer: ", log.LstdFlags)
	logger.Println("Запуск продюсера...")

	// Название топика
	topic := "basic-topic"

	// Создаем продюсера
	producer, err := kafka.NewProducer(topic, nil, logger)
	if err != nil {
		logger.Fatalf("Ошибка при создании продюсера: %v", err)
	}
	defer producer.Close()

	// Отправляем несколько сообщений
	for i := 1; i <= 10; i++ {
		message := fmt.Sprintf("Тестовое сообщение %d (время: %s)", i, time.Now().Format(time.RFC3339))

		if err := producer.Send(message, ""); err != nil {
			logger.Printf("Ошибка при отправке сообщения: %v", err)
			continue
		}

		// Делаем небольшую паузу между сообщениями
		time.Sleep(500 * time.Millisecond)
	}

	// Ждем, пока все сообщения будут отправлены
	producer.Flush()

	logger.Println("Все сообщения отправлены!")
}