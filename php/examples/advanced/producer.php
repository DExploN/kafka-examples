<?php

require_once __DIR__ . '/../../vendor/autoload.php';

use App\KafkaProducer;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Создаем логгер
$logger = new Logger('advanced-producer');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

// Название топика
$topic = 'advanced-topic';

// Создаем продюсера
$producer = new KafkaProducer($topic, [], $logger);

// Отправляем одиночное сообщение
$producer->send('Одиночное сообщение: ' . date('Y-m-d H:i:s'), 'single-key');

// Отправляем пакет сообщений
$messages = [
    'Пакетное сообщение 1: ' . date('Y-m-d H:i:s'),
    'Пакетное сообщение 2: ' . date('Y-m-d H:i:s'),
    'Пакетное сообщение 3: ' . date('Y-m-d H:i:s'),
];

$keys = ['batch-key-1', 'batch-key-2', 'batch-key-3'];

$count = $producer->sendBatch($messages, $keys);
$logger->info("Отправлено {$count} сообщений");

// Ждем, пока все сообщения будут отправлены
$producer->flush();

$logger->info('Работа продюсера завершена');
