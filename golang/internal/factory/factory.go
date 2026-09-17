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
const consumerName = "consumer-"

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, ch, err := dialAndConnectCh(connectionSettings.Hostname, connectionSettings.Port)

	if err != nil {
		return nil, err
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
	return closeChAndConn(q.channel, q.conn)
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
		return classifyError(err)
	}

	consumerTag := consumerName + q.queue

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
		return classifyError(err)
	}

	q.id = consumerTag
	q.consuming = true
	q.signal = consumeFrom(msgCh, callbackFunc)
	return nil

}

func consumeFrom(msgCh <-chan amqp.Delivery, callbackFunc func(msg m.Message, ack func(), nack func())) chan struct{} {
	signal := make(chan struct{})
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
	return signal
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

	if err != nil {
		return classifyError(err)
	}

	return nil

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
	conn, ch, err := dialAndConnectCh(connectionSettings.Hostname, connectionSettings.Port)

	if err != nil {
		return nil, err
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
	return closeChAndConn(e.channel, e.conn)
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

		if err != nil {
			return classifyError(err)
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
		return classifyError(err)
	}

	q, err := e.channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)

	if err != nil {
		return classifyError(err)
	}

	queue := q.Name

	err = bindKeys(e.channel, queue, e.exchange, e.keys)
	if err != nil {
		return err
	}

	consumerTag := consumerName + queue

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
		return classifyError(err)
	}

	e.id = consumerTag
	e.queue = queue
	e.consuming = true
	e.signal = consumeFrom(msgCh, callbackFunc)
	return nil

}

func bindKeys(channel *amqp.Channel, queue string, exchange string, keys []string) error {
	for _, key := range keys {
		err := channel.QueueBind(
			queue,
			key,
			exchange,
			false,
			nil)

		if err != nil {
			_, _ = channel.QueueDelete(queue, false, false, false)
			if errors.Is(err, amqp.ErrClosed) {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}
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

func closeChAndConn(channel *amqp.Channel, conn *amqp.Connection) error {
	chErr := channel.Close()
	conErr := conn.Close()

	if chErr != nil || conErr != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}

func classifyError(err error) error {
	if errors.Is(err, amqp.ErrClosed) {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

func dialAndConnectCh(hostname string, port int) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://%s:%d/", hostname, port)
	conn, err := amqp.Dial(url)

	if err != nil {
		return nil, nil, ErrCreateMiddlewareConn
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, ErrCreateMiddlewareChannel
	}

	return conn, ch, nil
}
