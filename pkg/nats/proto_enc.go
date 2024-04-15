package nats

import (
	"errors"
	"google.golang.org/protobuf/proto"
)

const PROTOBUF_ENCODER = "protobuf"

var (
	ErrUnableToCastProtobufType = errors.New("unable to type-cast to proto message")
)

// ProtobufEncoder is a Protobuf Encoder implementation for EncodedConn.
// This encoder will use the builtin github.com/golang/protobuf/proto to Marshal
// and Unmarshal most types, including structs.
type ProtobufEncoder struct {
	// Empty
}

// Encode ...
func (pe *ProtobufEncoder) Encode(subject string, v interface{}) ([]byte, error) {
	protoMsg, ok := v.(proto.Message)
	if !ok {
		return nil, ErrUnableToCastProtobufType
	}

	return proto.Marshal(protoMsg)
}

// Decode ...
func (pe *ProtobufEncoder) Decode(subject string, data []byte, vPtr interface{}) (err error) {
	protoMsgPtr, ok := vPtr.(proto.Message)
	if !ok {
		return ErrUnableToCastProtobufType
	}

	return proto.Unmarshal(data, protoMsgPtr)
}
