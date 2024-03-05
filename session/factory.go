package session

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"

	"github.com/Azure/go-amqp"
)

// ContainerFactory creates Containers with a given configuration using a
// specified container ID
type ContainerFactory interface {
	Create(id string) Container
}

func NewContainerFactory(address string, cfg ContainerConfig) ContainerFactory {
	return factory{
		Address: address,
		Config:  cfg,
	}
}

func NewMockContainerFactory() ContainerFactory {
	return mockFactory{Router: NewMockRouter()}
}

type mockFactory struct {
	Router *MockRouter
}

func (m mockFactory) Create(string) Container {
	return NewMockContainer(m.Router)
}

type factory struct {
	Address string
	Config  ContainerConfig
}

func (f factory) Create(id string) Container {
	config := f.Config
	// deep copy connection options
	var connOpts amqp.ConnOptions
	if config.Conn != nil {
		connOpts = *config.Conn
	}
	connOpts.ContainerID = randomSuffix(id)
	config.Conn = &connOpts
	return NewContainer(f.Address, config)
}

func randomSuffix(prefix string) string {
	var salt [4]byte
	io.ReadFull(rand.Reader, salt[:])
	out := bytes.NewBuffer([]byte(prefix))
	out.WriteByte('-')
	hex.NewEncoder(out).Write(salt[:])
	return out.String()
}
