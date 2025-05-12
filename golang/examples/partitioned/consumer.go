package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Создаем логгер
	logger := log.New(os.Stdout, "partitioned-consumer: ", log.LstdFlags)
	
	// Получаем номер партиции из аргументов командной строки или используем 0 по умолчанию
	partition := 0
	if len(os.Args) > 1 {
		partArg, err := strconv.Atoi(os.Args[1])
		if err != nil {
			logger.Fatalf("Некорректный номер партиции: %v", err)
		}
		partition = partArg
	}

	logger.Printf("Запуск консьюмера для партиции %d...", partition)

	// Название топика
	topic := "partitioned-topic-go"

	// Создаем конфигурацию
	config := kafka.ConfigMap{
		"bootstrap.servers":  "kafka:29092",
		"group.id":           fmt.Sprintf("partitioned-consumer-group-%d", partition),
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	}

	// Создаем консьюмера
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		logger.Fatalf("Ошибка при создании консьюмера: %v", err)
	}
	defer consumer.Close()

	// Создаем список партиций для конкретного топика
	topicPartitions := []kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: int32(partition),
			Offset:    kafka.OffsetBeginning,
		},
	}

	// Назначаем консьюмеру указанные партиции конкретных топиков
	err = consumer.Assign(topicPartitions)
	if err != nil {
		logger.Fatalf("Ошибка при назначении партиций: %v", err)
	}

	logger.Printf("Успешно подписались на топик %s, партиция %d", topic, partition)

	// Обработчик CTRL+C для грациозного завершения
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Флаг для контроля работы цикла
	running := true

	// Запускаем обработку в отдельной горутине
	go func() {
		<-sigchan
		logger.Println("Получен сигнал завершения")
		running = false
	}()

	// Основной цикл чтения
	for running {
		// Получаем сообщение с таймаутом
		msg, err := consumer.ReadMessage(100 * time.Millisecond)
		
		if err != nil {
			// Проверяем, является ли ошибка таймаутом
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue // Таймаут - не ошибка, продолжаем
			}
			logger.Printf("Ошибка при чтении сообщения: %v", err)
			continue
		}

		// Выводим полученное сообщение
		logger.Printf("Получено сообщение из топика %s [%d] со смещением %v:\n  Ключ: %s\n  Значение: %s",
			*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset,
			string(msg.Key), string(msg.Value))
	}

	logger.Println("Консьюмер остановлен")
}