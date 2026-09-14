package factory

import (
	"context"
	"errors"
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrCreateMiddlewareConn    = errors.New("create middleware: conn failed")
	ErrCreateMiddlewareChannel = errors.New("create middleware: create channel failed")
	ErrCreateMiddlewareDeclare = errors.New("create middleware: declare failed")
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrCreateMiddlewareConn
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, ErrCreateMiddlewareChannel
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		return nil, ErrCreateMiddlewareDeclare
	}

	return &queueMiddleware{conn: conn, channel: ch, queue: q.Name}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrCreateMiddlewareConn
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, ErrCreateMiddlewareChannel
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return nil, ErrCreateMiddlewareDeclare
	}

	return &exchangeMiddleware{conn: conn, channel: ch, exchange: exchange, keys: keys}, nil
}

type exchangeMiddleware struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	exchange string
	keys     []string
}

// Close implements [middleware.Middleware].
func (e *exchangeMiddleware) Close() error {
	panic("unimplemented")
}

// Send implements [middleware.Middleware].
func (e *exchangeMiddleware) Send(msg m.Message) error {
	panic("unimplemented")
}

// StartConsuming implements [middleware.Middleware].
func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	panic("unimplemented")
}

// StopConsuming implements [middleware.Middleware].
func (e *exchangeMiddleware) StopConsuming() error {
	panic("unimplemented")
}

type queueMiddleware struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
}

// Close implements [middleware.Middleware].
func (q *queueMiddleware) Close() error {
	panic("unimplemented")
}

// StartConsuming implements [middleware.Middleware].
func (q *queueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	panic("unimplemented")
}

// StopConsuming implements [middleware.Middleware].
func (q *queueMiddleware) StopConsuming() error {
	panic("unimplemented")
}

func (q *queueMiddleware) Send(msg m.Message) error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := q.channel.PublishWithContext(ctx,
		"",      // exchange
		q.queue, // routing key
		false,   // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		})

	if err == nil {
		return nil
	}

	if errors.Is(err, amqp.ErrClosed) {
		return m.ErrMessageMiddlewareDisconnected
	}

	return m.ErrMessageMiddlewareMessage
}
