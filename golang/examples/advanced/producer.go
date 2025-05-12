package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kafka-examples/golang/src/kafka"
)

func main() {
	// Создаем логгер
	logger := log.New(os.Stdout, "advanced-producer: ", log.LstdFlags)
	logger.Println("Запуск продвинутого продюсера...")

	// Название топика
	topic := "advanced-topic"

	// Создаем продюсера с расширенной конфигурацией
	producer, err := kafka.NewProducer(topic, map[string]string{
		"acks": "all", // Ожидаем подтверждения от всех реплик
	}, logger)
	if err != nil {
		logger.Fatalf("Ошибка при создании продюсера: %v", err)
	}
	defer producer.Close()

	// Запускаем обработку отчетов о доставке
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer.ProcessDeliveryReports(ctx)

	// Отправляем одиночное сообщение с ключом
	singleMsg := fmt.Sprintf("Одиночное сообщение: %s", time.Now().Format(time.RFC3339))
	singleKey := "single-key"
	
	logger.Printf("Отправка одиночного сообщения с ключом: %s", singleKey)
	if err := producer.Send(singleMsg, singleKey); err != nil {
		logger.Printf("Ошибка при отправке сообщения: %v", err)
	}

	// Отправляем пакет сообщений с ключами
	messages := []string{
		fmt.Sprintf("Пакетное сообщение 1: %s", time.Now().Format(time.RFC3339)),
		fmt.Sprintf("Пакетное сообщение 2: %s", time.Now().Format(time.RFC3339)),
		fmt.Sprintf("Пакетное сообщение 3: %s", time.Now().Format(time.RFC3339)),
	}

	keys := []string{
		"batch-key-1",
		"batch-key-2",
		"batch-key-3",
	}

	logger.Println("Отправка пакета сообщений")
	count, err := producer.SendBatch(messages, keys)
	if err != nil {
		logger.Printf("Ошибка при отправке пакета сообщений: %v", err)
	} else {
		logger.Printf("Отправлено %d сообщений", count)
	}

	// Ждем, пока все сообщения будут отправлены
	producer.Flush()

	// Делаем небольшую паузу, чтобы увидеть все отчеты о доставке
	time.Sleep(1 * time.Second)

	logger.Println("Работа продюсера завершена!")
}