package main

import (
	"encoding/binary"
	"testing"
)

func field(n byte, b []byte) []byte {
	out := []byte{n<<3 | 2}
	out = binary.AppendUvarint(out, uint64(len(b)))
	return append(out, b...)
}
func protobufCM(value string) []byte {
	entry := append(field(1, []byte("intents.json")), field(2, []byte(value))...)
	return append([]byte{'k', '8', 's', 0}, field(2, field(2, entry))...)
}
func TestJournalEmpty(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
		want bool
	}{
		{"json", []byte(`{"data":{"intents.json":"{}"}}`), true},
		{"protobuf", protobufCM("{}"), true},
		{"whitespace", protobufCM("{ }"), true},
		{"pending", protobufCM(`{"uid":{}}`), false},
		{"null", protobufCM("null"), false},
		{"wrong field", []byte(`{"metadata":{"name":"{}"}}`), false},
		{"truncated", []byte{'k', '8', 's', 0, 18, 255}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := journalEmpty(tc.body); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
