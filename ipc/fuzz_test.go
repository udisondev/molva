package ipc

import (
	"testing"

	"github.com/udisondev/molva/proto/ipcpb"
	"google.golang.org/protobuf/proto"
)

// FuzzDecodeFrame: кадры от renderer'а — недоверенный ввод; произвольные
// байты не паникуют, успех переживает round-trip.
func FuzzDecodeFrame(f *testing.F) {
	f.Add([]byte{})
	valid, _ := EncodeFrame(&ipcpb.Frame{Kind: &ipcpb.Frame_Hello{Hello: &ipcpb.Hello{Token: []byte("t")}}})
	f.Add(valid)
	cmd, _ := EncodeFrame(&ipcpb.Frame{Kind: &ipcpb.Frame_Command{Command: &ipcpb.Command{
		Id: 7, Kind: &ipcpb.Command_SendText{SendText: &ipcpb.SendText{Peer: []byte{1}, Text: "x"}},
	}}})
	f.Add(cmd)
	f.Add([]byte{0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		fr, err := DecodeFrame(data)
		if err != nil {
			return
		}
		b, err := EncodeFrame(fr)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		fr2, err := DecodeFrame(b)
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if !proto.Equal(fr, fr2) {
			t.Fatal("round-trip разошёлся")
		}
	})
}

// FuzzDecodeMediaFrame: медиакадры от renderer'а — недоверенный ввод;
// произвольные байты не паникуют, успех переживает round-trip.
func FuzzDecodeMediaFrame(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{TagMedia, 16})
	valid, _ := EncodeMediaFrame(nil, 16, 1234, []byte("opus"))
	f.Add(valid)
	f.Add(append([]byte{TagMedia, 17, 0, 0, 0, 0, 0, 0, 0, 0}, make([]byte, 100)...))

	f.Fuzz(func(t *testing.T, data []byte) {
		ch, rx, payload, err := DecodeMediaFrame(data)
		if err != nil {
			return
		}
		b, err := EncodeMediaFrame(nil, ch, rx, payload)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		ch2, rx2, payload2, err := DecodeMediaFrame(b)
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if ch2 != ch || rx2 != rx || string(payload2) != string(payload) {
			t.Fatal("round-trip разошёлся")
		}
	})
}
