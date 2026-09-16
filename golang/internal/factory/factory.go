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
		nil,
	)
	if err != nil {
		return nil, ErrCreateMiddlewareDeclare
	}

	return &queueMiddleware{conn: conn, channel: ch, queue: q.Name, consuming: false, id: ""}, nil
}

type queueMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queue     string
	consuming bool
	id        string
}

// Close implements [middleware.Middleware].
func (q *queueMiddleware) Close() error {
	if err := q.channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := q.conn.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

// StartConsuming implements [middleware.Middleware].
func (q *queueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if q.consuming {
		return nil
	}

	err := q.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	consumerTag := "consumer-queue-" + q.queue

	msgCh, err := q.channel.Consume(
		q.queue,     // queue
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	q.id = consumerTag
	q.consuming = true

	go func() {
		for msgD := range msgCh {
			msg := m.Message{Body: string(msgD.Body)}

			ack := func() {
				_ = msgD.Ack(false)
			}

			nack := func() {
				_ = msgD.Nack(false, true)
			}

			callbackFunc(msg, ack, nack)
		}
	}()

	return nil

}

// StopConsuming implements [middleware.Middleware].
func (q *queueMiddleware) StopConsuming() error {
	if q.id == "" {
		return nil
	}

	if q.channel == nil || q.channel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	if err := q.channel.Cancel(q.id, false); err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	q.id = ""
	q.consuming = false

	return nil
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

type exchangeMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	exchange  string
	keys      []string
	consuming bool
	id        string
	queue     string
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

	return &exchangeMiddleware{conn: conn, channel: ch, exchange: exchange, keys: keys, consuming: false, id: ""}, nil
}

// Close implements [middleware.Middleware].
func (e *exchangeMiddleware) Close() error {
	if err := e.channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := e.conn.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil

}

// Send implements [middleware.Middleware].
func (e *exchangeMiddleware) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range e.keys {
		err := e.channel.PublishWithContext(ctx,
			e.exchange, // exchange
			key,        // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})

		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}

		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	return nil
}

// StartConsuming implements [middleware.Middleware].
func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {

	if e.consuming {
		return nil
	}

	err := e.channel.Qos(
		10,    // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	q, err := e.channel.QueueDeclare(
		"",    // name
		false, // durability
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	queue := q.Name
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	for _, key := range e.keys {
		err = e.channel.QueueBind(
			queue,      // queue name
			key,        // routing key
			e.exchange, // exchange
			false,
			nil)

		if err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}

	consumerTag := "consumer-exchange-" + queue

	msgCh, err := e.channel.Consume(
		queue,       // queue
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	e.id = consumerTag
	e.queue = queue
	e.consuming = true

	go func() {
		for msgD := range msgCh {
			msg := m.Message{Body: string(msgD.Body)}

			ack := func() {
				_ = msgD.Ack(false)
			}

			nack := func() {
				_ = msgD.Nack(false, true)
			}

			callbackFunc(msg, ack, nack)
		}
	}()

	return nil

}

// StopConsuming implements [middleware.Middleware].
func (e *exchangeMiddleware) StopConsuming() error {
	if e.id == "" || !e.consuming {
		return nil
	}

	if e.channel == nil || e.channel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	if err := e.channel.Cancel(e.id, false); err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	e.id = ""
	e.consuming = false

	return nil

}
