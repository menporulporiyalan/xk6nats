package nats

import (
	"crypto/tls"
	"fmt"

	"github.com/dop251/goja"
	natsio "github.com/nats-io/nats.go"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/nats", new(RootModule))
}

// RootModule is the global module object type. It is instantiated once per test
// run and will be used to create k6/x/nats module instances for each VU.
type RootModule struct{}

// ModuleInstance represents an instance of the module for every VU.
type Nats struct {
	conn    *natsio.Conn
	vu      modules.VU
	exports map[string]interface{}
}

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Instance = &Nats{}
	_ modules.Module   = &RootModule{}
)

// NewModuleInstance implements the modules.Module interface and returns
// a new instance for each VU.
func (r *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	mi := &Nats{
		vu:      vu,
		exports: make(map[string]interface{}),
	}

	mi.exports["Nats"] = mi.client

	return mi
}

// Exports implements the modules.Instance interface and returns the exports
// of the JS module.
func (mi *Nats) Exports() modules.Exports {
	return modules.Exports{
		Named: mi.exports,
	}
}

func (n *Nats) client(c goja.ConstructorCall) *goja.Object {
	rt := n.vu.Runtime()

	var cfg Configuration
	err := rt.ExportTo(c.Argument(0), &cfg)
	if err != nil {
		common.Throw(rt, fmt.Errorf("Nats constructor expect Configuration as it's argument: %w", err))
	}

	natsOptions := natsio.GetDefaultOptions()
	natsOptions.Servers = cfg.Servers
	if cfg.Unsafe {
		natsOptions.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	if cfg.Token != "" {
		natsOptions.Token = cfg.Token
	}

	conn, err := natsOptions.Connect()
	if err != nil {
		common.Throw(rt, err)
	}

	return rt.ToValue(&Nats{
		vu:   n.vu,
		conn: conn,
	}).ToObject(rt)
}

func (n *Nats) Close() {
	if n.conn != nil {
		n.conn.Close()
	}
}

func (n *Nats) Subscribe(topic string, handler MessageHandler) (*Subscription, error) {
	if n.conn == nil {
		return nil, fmt.Errorf("the connection is not valid")
	}

	sub, err := n.conn.Subscribe(topic, func(msg *natsio.Msg) {
		msg.Ack()
		h := make(map[string]string)
		for k := range msg.Header {
			h[k] = msg.Header.Get(k)
		}

		message := Message{
			Topic:  msg.Subject,
		}
		handler(message)
	})

	if err != nil {
		return nil, err
	}

	subscription := Subscription{
		Close: func() error {
			return sub.Unsubscribe()
		},
	}

	return &subscription, err
}



func (n *Nats) Asyncsubscribe(topic string, handler MessageHandler) (*Subscription, error) {
	if n.conn == nil {
		return nil, fmt.Errorf("the connection is not valid")
	}

	sub, err := n.conn.Subscribe(topic, func(msg *natsio.Msg) {
		msg.Ack()
		h := make(map[string]string)
		for k := range msg.Header {
			h[k] = msg.Header.Get(k)
		}

		message := {msg.Subject}
		// message := Message{
		// 	Topic:  msg.Subject,
		// }
		handler(message)
	})

	if err != nil {
		return nil, err
	}

	subscription := Subscription{
		Close: func() error {
			return sub.Unsubscribe()
		},
	}

	return &subscription, err
}

type Configuration struct {
	Servers []string
	Unsafe  bool
	Token   string
}

type Message struct {
	Topic  string
}

type Subscription struct {
	Close func() error
}

type MessageHandler func(Message)