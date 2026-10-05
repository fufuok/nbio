package nbhttp

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type retryListener struct {
	entered chan struct{}
	once    sync.Once
	closed  uint32
}

type retryAcceptError struct{}

// Error describes the simulated transient listener failure.
func (retryAcceptError) Error() string { return "temporary accept failure" }

// Timeout selects the existing accept-loop retry path.
func (retryAcceptError) Timeout() bool { return true }

// Temporary satisfies net.Error for the simulated retry condition.
func (retryAcceptError) Temporary() bool { return true }

// Accept signals that the loop has started, then requests another retry.
func (l *retryListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.entered) })
	return nil, retryAcceptError{}
}

// Close records cleanup without waking the retry loop; its exit must follow the
// engine stop flag rather than a test channel that would synchronize that flag.
func (l *retryListener) Close() error {
	atomic.StoreUint32(&l.closed, 1)
	return nil
}

// Addr supplies an address without opening a socket or reserving a fixed port.
func (l *retryListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

// TestEngineStopDuringAcceptRetry checks both stop-flag writers while a
// nonblocking listener retries a transient error. It verifies bounded shutdown;
// upstream //go:norace directives limit what an unmodified baseline race run can detect.
func TestEngineStopDuringAcceptRetry(t *testing.T) {
	for _, graceful := range []bool{false, true} {
		name := "Stop"
		if graceful {
			name = "Shutdown"
		}
		t.Run(name, func(t *testing.T) {
			listener := &retryListener{entered: make(chan struct{})}
			engine := NewEngine(Config{
				Addrs: []string{"127.0.0.1:0"}, IOMod: IOModNonBlocking, NPoller: 1,
				Listen: func(string, string) (net.Listener, error) { return listener, nil },
			})
			if err := engine.Start(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-listener.entered:
			case <-time.After(3 * time.Second):
				engine.Stop()
				t.Fatal("listener did not enter Accept")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stopped := make(chan error, 1)
			go func() {
				if graceful {
					stopped <- engine.Shutdown(ctx)
					return
				}
				engine.Stop()
				stopped <- nil
			}()
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatalf("shutdown failed: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("engine did not stop while Accept was retrying")
			}
			if atomic.LoadUint32(&listener.closed) != 1 {
				t.Fatal("listener was not closed")
			}
		})
	}
}
