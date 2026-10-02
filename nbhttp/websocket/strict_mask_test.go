package websocket

import (
	"errors"
	"testing"
)

// TestNextFrameEnforcesMaskDirection verifies both valid and invalid mask directions.
func TestNextFrameEnforcesMaskDirection(t *testing.T) {
	tests := []struct {
		name     string
		isClient bool
		masked   bool
		wantErr  bool
	}{
		{name: "server accepts masked", isClient: false, masked: true},
		{name: "server rejects unmasked", isClient: false, masked: false, wantErr: true},
		{name: "client accepts unmasked", isClient: true, masked: false},
		{name: "client rejects masked", isClient: true, masked: true, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := []byte{0x81, 1}
			if test.masked {
				frame[1] |= maskBit
				frame = append(frame, 0, 0, 0, 0)
			}
			frame = append(frame, 'x')
			conn := &Conn{commonFields: &commonFields{MessageLengthLimit: 1024}, isClient: test.isClient, bytesCached: &frame}
			_, _, _, _, _, _, err := conn.nextFrame()
			if test.wantErr {
				if !errors.Is(err, ErrInvalidMaskDirection) {
					t.Fatalf("nextFrame error = %v, want %v", err, ErrInvalidMaskDirection)
				}
				return
			}
			if err != nil {
				t.Fatalf("nextFrame returned unexpected error: %v", err)
			}
		})
	}
}
