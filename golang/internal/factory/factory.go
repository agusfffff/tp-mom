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

const publishTimeout = 5 * time.Second

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrCreateMiddlewareConn
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, ErrCreateMiddlewareChannel
	}

	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
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
	signal    chan struct{}
}

// Close implements [middleware.Middleware].
func (q *queueMiddleware) Close() error {
	chErr := q.channel.Close()
	conErr := q.conn.Close()

	if chErr != nil || conErr != nil {
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
		1,
		0,
		false,
	)

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	consumerTag := "consumer-queue-" + q.queue

	msgCh, err := q.channel.Consume(
		q.queue,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	q.id = consumerTag
	q.consuming = true
	signal := make(chan struct{})
	q.signal = signal

	go func() {
		defer close(signal)
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
		return m.ErrMessageMiddlewareClose
	}

	<-q.signal
	q.signal = nil
	q.id = ""
	q.consuming = false

	return nil
}

func (q *queueMiddleware) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)

	defer cancel()

	err := q.channel.PublishWithContext(ctx,
		"",
		q.queue,
		false,
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
	signal    chan struct{}
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrCreateMiddlewareConn
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, ErrCreateMiddlewareChannel
	}

	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, ErrCreateMiddlewareDeclare
	}

	return &exchangeMiddleware{conn: conn, channel: ch, exchange: exchange, keys: keys, consuming: false, id: ""}, nil
}

// Close implements [middleware.Middleware].
func (e *exchangeMiddleware) Close() error {
	chErr := e.channel.Close()
	conErr := e.conn.Close()

	if chErr != nil || conErr != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}

// Send implements [middleware.Middleware].
func (e *exchangeMiddleware) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	for _, key := range e.keys {
		err := e.channel.PublishWithContext(ctx,
			e.exchange,
			key,
			false,
			false,
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
		10,
		0,
		false,
	)

	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	q, err := e.channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
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
			queue,
			key,
			e.exchange,
			false,
			nil)

		if err != nil {
			_, _ = e.channel.QueueDelete(queue, false, false, false)
			if errors.Is(err, amqp.ErrClosed) {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}

	consumerTag := "consumer-exchange-" + queue

	msgCh, err := e.channel.Consume(
		queue,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		_, _ = e.channel.QueueDelete(queue, false, false, false)
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	e.id = consumerTag
	e.queue = queue
	e.consuming = true
	signal := make(chan struct{})
	e.signal = signal

	go func() {
		defer close(signal)
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
		return m.ErrMessageMiddlewareClose
	}

	<-e.signal
	e.signal = nil
	e.id = ""
	e.consuming = false

	return nil

}
