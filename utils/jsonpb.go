package utils

import (
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

var marshaler = jsonpb.Marshaler{Indent: "\t"}

func MarshalJsonPB(i proto.Message) (string, error) {
	return marshaler.MarshalToString(i)
}

func Unmarshal(s string, message proto.Message) error {
	return jsonpb.UnmarshalString(s, message)
}
