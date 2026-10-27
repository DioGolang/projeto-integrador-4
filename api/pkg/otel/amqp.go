package otel

import amqp "github.com/rabbitmq/amqp091-go"

// AMQPHeadersCarrier adapts amqp.Table to the propagation.TextMapCarrier interface.
type AMQPHeadersCarrier amqp.Table

// NewAMQPHeadersCarrier creates a valid non-nil carrier from an amqp.Table.
func NewAMQPHeadersCarrier(table amqp.Table) AMQPHeadersCarrier {
	if table == nil {
		return AMQPHeadersCarrier(make(amqp.Table))
	}
	return AMQPHeadersCarrier(table)
}

func (c AMQPHeadersCarrier) Get(key string) string {
	if c == nil {
		return ""
	}
	v, ok := c[key]
	if !ok || v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	}
	return ""
}

func (c AMQPHeadersCarrier) Set(key string, value string) {
	if c != nil {
		c[key] = value
	}
}

func (c AMQPHeadersCarrier) Keys() []string {
	if c == nil {
		return nil
	}
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// Table returns the underlying amqp.Table.
func (c AMQPHeadersCarrier) Table() amqp.Table {
	if c == nil {
		return amqp.Table{}
	}
	return amqp.Table(c)
}
