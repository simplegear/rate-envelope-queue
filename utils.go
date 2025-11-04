package rate_envelope_queue

import (
	"context"
	"encoding/binary"
	"log"
	"runtime/debug"
	"time"
)

func withHookTimeout(ctx context.Context, base time.Duration, frac float64, min time.Duration) (context.Context, context.CancelFunc) {
	d := time.Duration(float64(base) * frac)
	if d < min {
		d = min
	}
	if base == 0 {
		d = min
	}
	return context.WithTimeout(ctx, d)
}

func recoverWrap() {
	if r := recover(); r != nil {
		log.Printf(service+": panic recovered: %v\n%s", r, debug.Stack())
	}
}

func Uint64ToBytesBE(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

func Uint64ToBytesLE(v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return b[:]
}

func BytesToUint64BE(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func BytesToUint64LE(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
