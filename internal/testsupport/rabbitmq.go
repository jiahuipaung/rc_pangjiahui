package testsupport

import "os"

func RabbitMQURL() string { return os.Getenv("TEST_RABBITMQ_URL") }
