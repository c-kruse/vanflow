package vanflow

import (
	"errors"
	"fmt"

	"github.com/c-kruse/vanflow/encoding"
	"github.com/interconnectedcloud/go-amqp"
)

func Decode(msg *amqp.Message) (interface{}, error) {
	if msg == nil || msg.Properties == nil {
		return nil, errors.New("cannot decode message with nil properties")
	}
	switch msg.Properties.Subject {
	case "BEACON":
		return DecodeBeacon(msg), nil
	case "HEARTBEAT":
		return DecodeHeartbeat(msg), nil
	case "FLUSH":
		return DecodeFlush(msg), nil
	case "RECORD":
		return DecodeRecord(msg)
	default:
		return nil, fmt.Errorf("cannot decode message with subject %q", msg.Properties.Subject)
	}
}

type BeaconMessage struct {
	MessageProps
	Version    uint32
	SourceType string
	Address    string
	Direct     string
	Identity   string
}

func DecodeBeacon(msg *amqp.Message) BeaconMessage {
	var m BeaconMessage
	m.To = msg.Properties.To
	m.Subject = msg.Properties.Subject
	m.ReplyTo = msg.Properties.ReplyTo
	if version, ok := msg.ApplicationProperties["v"].(uint32); ok {
		m.Version = version
	}
	if sourceType, ok := msg.ApplicationProperties["sourceType"].(string); ok {
		m.SourceType = sourceType
	}
	if address, ok := msg.ApplicationProperties["address"].(string); ok {
		m.Address = address
	}
	if direct, ok := msg.ApplicationProperties["direct"].(string); ok {
		m.Direct = direct
	}
	if identity, ok := msg.ApplicationProperties["id"].(string); ok {
		m.Identity = identity
	}
	return m
}

func (m BeaconMessage) Encode() *amqp.Message {
	return &amqp.Message{
		Properties: &amqp.MessageProperties{
			To:      "mc/sfe.all",
			Subject: "BEACON",
		},
		ApplicationProperties: map[string]interface{}{
			"v":          m.Version,
			"sourceType": m.SourceType,
			"address":    m.Address,
			"direct":     m.Direct,
			"id":         m.Identity,
		},
	}
}

type MessageProps struct {
	To      string
	Subject string
	ReplyTo string
}

type HeartbeatMessage struct {
	MessageProps
	Identity string
	Version  uint32
	Now      uint64
}

func DecodeHeartbeat(msg *amqp.Message) HeartbeatMessage {
	var m HeartbeatMessage
	m.To = msg.Properties.To
	m.Subject = msg.Properties.Subject

	if version, ok := msg.ApplicationProperties["v"].(uint32); ok {
		m.Version = version
	}
	if now, ok := msg.ApplicationProperties["now"].(uint64); ok {
		m.Now = now
	}
	if identity, ok := msg.ApplicationProperties["id"].(string); ok {
		m.Identity = identity
	}
	return m
}

func (m HeartbeatMessage) Encode() *amqp.Message {
	return &amqp.Message{
		Properties: &amqp.MessageProperties{
			To:      "mc/sfe." + m.Identity,
			Subject: "HEARTBEAT",
		},
		ApplicationProperties: map[string]interface{}{
			"v":   m.Version,
			"now": m.Now,
			"id":  m.Identity,
		},
	}
}

type FlushMessage struct {
	MessageProps
}

func DecodeFlush(msg *amqp.Message) FlushMessage {
	var flush FlushMessage
	flush.To = msg.Properties.To
	flush.Subject = msg.Properties.Subject
	flush.ReplyTo = msg.Properties.ReplyTo
	return flush
}

func (m FlushMessage) Encode() *amqp.Message {
	return &amqp.Message{
		Properties: &amqp.MessageProperties{
			To:      m.To,
			ReplyTo: m.ReplyTo,
			Subject: "FLUSH",
		},
	}
}

type RecordMessage struct {
	MessageProps
	Records []Record
}

func DecodeRecord(msg *amqp.Message) (record RecordMessage, err error) {
	record.To = msg.Properties.To
	record.Subject = msg.Properties.Subject
	record.Records, err = decodeRecords(msg)
	return record, err
}

func (m RecordMessage) Encode() (*amqp.Message, error) {
	var records []interface{}
	for i, record := range m.Records {
		recordAttrs, err := encoding.Encode(record)
		if err != nil {
			return nil, fmt.Errorf("error encoding record %d: %s", i, err)
		}
		records = append(records, recordAttrs)
	}
	return &amqp.Message{
		Properties: &amqp.MessageProperties{
			To:      m.To,
			Subject: "RECORD",
		},
		Value: records,
	}, nil
}

// decodeRecords decodes an AMQP Message into a set of Records. Uses the
// recordDecoders map to find the correct decoder for each record type.
func decodeRecords(msg *amqp.Message) ([]Record, error) {
	var records []Record
	values, ok := msg.Value.([]interface{})
	if !ok {
		return records, fmt.Errorf("unexpected type for message Value: %T", msg.Value)
	}
	for _, value := range values {
		recordAttributes, ok := value.(map[interface{}]interface{})
		if !ok {
			return records, fmt.Errorf("unexpected type in message Value slice: %T", value)
		}
		record, err := encoding.Decode(recordAttributes)
		if err != nil {
			return records, err
		}
		r, ok := record.(Record)
		if !ok {
			return records, fmt.Errorf("decoded type does not implement Record: %T", record)
		}
		records = append(records, r)
	}
	return records, nil
}
