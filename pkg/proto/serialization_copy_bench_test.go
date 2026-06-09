package proto

import (
	"bytes"
	"io"
	"testing"

	"go.gmountie.dev/gmountie/pkg/common/grpc/snappy"

	"google.golang.org/grpc/encoding"
	gproto "google.golang.org/protobuf/proto"
)

// These benchmarks verify, empirically, how many times a file-data payload is
// copied/allocated as it crosses the gRPC serialization boundary. They are
// pure protobuf + the registered snappy codec — no FUSE, no network — so they
// run anywhere and isolate the serialization cost from the syscall cost.
//
// The claim under test: protobuf-go has no zero-copy path for `bytes` fields.
// proto.Marshal copies Data into the wire buffer; proto.Unmarshal allocates a
// fresh []byte for Data and copies into it. Watch the B/op column: it tracks
// the 1 MiB payload, which is the copy.

const benchPayload = 1 << 20 // 1 MiB — matches the server's default FrameSizeBytes

func makeReadFrame() *ReadFrame {
	return &ReadFrame{Data: make([]byte, benchPayload), Status: 0}
}

// BenchmarkReadFrameMarshal proves Marshal copies the payload into the wire
// buffer: expect B/op >= benchPayload.
func BenchmarkReadFrameMarshal(b *testing.B) {
	frame := makeReadFrame()
	b.ReportAllocs()
	b.SetBytes(benchPayload)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := gproto.Marshal(frame)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) < benchPayload {
			b.Fatalf("short marshal: %d", len(out))
		}
	}
}

// BenchmarkReadFrameUnmarshal proves Unmarshal allocates a fresh slice for the
// payload and copies into it: expect B/op >= benchPayload.
func BenchmarkReadFrameUnmarshal(b *testing.B) {
	wire, err := gproto.Marshal(makeReadFrame())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(benchPayload)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var frame ReadFrame
		if err := gproto.Unmarshal(wire, &frame); err != nil {
			b.Fatal(err)
		}
		if len(frame.Data) != benchPayload {
			b.Fatalf("short unmarshal: %d", len(frame.Data))
		}
	}
}

// BenchmarkSnappyRoundtrip measures the cost of the registered snappy codec on
// an incompressible (random-ish) marshaled frame. The codec reads the whole
// marshaled buffer and writes a separate compressed buffer on each side, so
// the payload is touched again per direction on top of protobuf's copies.
func BenchmarkSnappyRoundtrip(b *testing.B) {
	wire, err := gproto.Marshal(makeReadFrame())
	if err != nil {
		b.Fatal(err)
	}
	// Fill with incompressible bytes so we measure copy cost, not the
	// best-case all-zeros compression shortcut.
	for i := range wire {
		wire[i] = byte(i * 31)
	}
	c := encoding.GetCompressor(snappy.Name) // registered via snappy package init
	if c == nil {
		b.Fatal("snappy compressor not registered")
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w, err := c.Compress(&buf)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := w.Write(wire); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		r, err := c.Decompress(bytes.NewReader(buf.Bytes()))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, r); err != nil {
			b.Fatal(err)
		}
	}
}
