// RabbitMQ tutorial scenarios with amqp091-go against FireMQ.
// Run: AMQP_URL=amqp://guest:guest@localhost:5672/ go test ./...
package compat

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func url() string {
	if u := os.Getenv("AMQP_URL"); u != "" {
		return u
	}
	return "amqp://guest:guest@localhost:5672/"
}

func setup(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(url())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, ch
}

func name(p string) string { return fmt.Sprintf("%s-%08x", p, rand.Uint32()) }

func publish(t *testing.T, ch *amqp.Channel, ex, key, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, ex, key, false, false, amqp.Publishing{Body: []byte(body)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func recv(t *testing.T, msgs <-chan amqp.Delivery) amqp.Delivery {
	t.Helper()
	select {
	case m := <-msgs:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a delivery")
		return amqp.Delivery{}
	}
}

func TestHelloWorld(t *testing.T) {
	_, ch := setup(t)
	q, err := ch.QueueDeclare(name("hello"), false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	publish(t, ch, "", q.Name, "Hello World!")
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(recv(t, msgs).Body); got != "Hello World!" {
		t.Fatalf("got %q", got)
	}
}

func TestWorkQueues(t *testing.T) {
	_, ch := setup(t)
	q, _ := ch.QueueDeclare(name("tasks"), true, false, false, false, nil)
	for i := 0; i < 5; i++ {
		publish(t, ch, "", q.Name, "task "+strconv.Itoa(i))
	}
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatal(err)
	}
	msgs, _ := ch.Consume(q.Name, "", false, false, false, false, nil)
	for i := 0; i < 5; i++ {
		m := recv(t, msgs)
		if string(m.Body) != "task "+strconv.Itoa(i) {
			t.Fatalf("got %q", m.Body)
		}
		m.Ack(false)
	}
}

func TestTopics(t *testing.T) {
	_, ch := setup(t)
	ex := name("topic_logs")
	if err := ch.ExchangeDeclare(ex, "topic", false, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	q, _ := ch.QueueDeclare("", false, false, true, false, nil)
	if err := ch.QueueBind(q.Name, "*.critical", ex, false, nil); err != nil {
		t.Fatal(err)
	}
	publish(t, ch, ex, "kern.critical", "yes")
	publish(t, ch, ex, "kern.info", "no")
	msgs, _ := ch.Consume(q.Name, "", true, false, false, false, nil)
	if got := string(recv(t, msgs).Body); got != "yes" {
		t.Fatalf("got %q", got)
	}
	select {
	case m := <-msgs:
		t.Fatalf("unexpected %q", m.Body)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPublisherConfirms(t *testing.T) {
	_, ch := setup(t)
	q, _ := ch.QueueDeclare(name("confirmed"), true, false, false, false, nil)
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 100))
	for i := 0; i < 50; i++ {
		publish(t, ch, "", q.Name, strconv.Itoa(i))
	}
	for i := 0; i < 50; i++ {
		select {
		case c := <-confirms:
			if !c.Ack {
				t.Fatalf("nack for %d", c.DeliveryTag)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after %d confirms", i)
		}
	}
}

func TestMandatoryReturn(t *testing.T) {
	_, ch := setup(t)
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, "", name("nowhere"), true, false, amqp.Publishing{Body: []byte("lost")}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-returns:
		if r.ReplyCode != 312 || string(r.Body) != "lost" {
			t.Fatalf("unexpected return %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no return")
	}
}

func TestInequivalentRedeclare(t *testing.T) {
	conn, ch := setup(t)
	q := name("strict")
	if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	_, err := ch.QueueDeclare(q, true, false, false, false, nil)
	amqpErr, ok := err.(*amqp.Error)
	if !ok || amqpErr.Code != 406 {
		t.Fatalf("expected 406, got %v", err)
	}
	if conn.IsClosed() {
		t.Fatal("a channel error closed the connection")
	}
}
