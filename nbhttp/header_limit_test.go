package nbhttp

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type headerBudgetProcessor struct {
	EmptyProcessor
	completed int
}

// OnComplete counts message boundaries to verify that each message gets a fresh budget.
func (p *headerBudgetProcessor) OnComplete(*Parser) { p.completed++ }

// TestParserHeaderBudget checks early rejection, reset, and body exclusion with fragmented input.
func TestParserHeaderBudget(t *testing.T) {
	cases := []struct {
		name      string
		wire      string
		limit     int
		wantErr   bool
		wantCount int
	}{
		{
			name:      "fragmented exact and reset",
			limit:     len("GET / HTTP/1.1\r\nHost: test\r\n\r\n"),
			wire:      "GET / HTTP/1.1\r\nHost: test\r\n\r\nGET / HTTP/1.1\r\nHost: test\r\n\r\n",
			wantCount: 2,
		},
		{
			name:      "body excluded",
			limit:     len("POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 8\r\n\r\n"),
			wire:      "POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 8\r\n\r\n12345678",
			wantCount: 1,
		},
		{
			name:    "fragmented overflow",
			limit:   len("GET / HTTP/1.1\r\nHost: test\r\n"),
			wire:    "GET / HTTP/1.1\r\nHost: test\r\nX-Fill: 1234567890\r\n\r\n",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := []byte(tc.wire)
			limit := tc.limit
			engine := &Engine{Config: Config{MaxHTTPHeaderSize: limit}, emptyRequest: &http.Request{}}
			processor := &headerBudgetProcessor{}
			parser := NewParser(nil, engine, processor, false, nil)
			defer parser.CloseAndClean(nil)
			var gotErr error
			for _, b := range wire {
				gotErr = parser.Parse([]byte{b})
				if gotErr != nil {
					break
				}
			}
			if tc.wantErr {
				if !errors.Is(gotErr, ErrTooLong) {
					t.Fatalf("error = %v, want ErrTooLong", gotErr)
				}
				if processor.completed != 0 {
					t.Fatalf("completed = %d, want 0", processor.completed)
				}
				return
			}
			if gotErr != nil || processor.completed != tc.wantCount {
				t.Fatalf("error = %v, completed = %d, want %d", gotErr, processor.completed, tc.wantCount)
			}
		})
	}
}

// TestNewEngineDefaultHeaderBudget checks that the constructor enables a separate header budget.
func TestNewEngineDefaultHeaderBudget(t *testing.T) {
	engine := NewEngine(Config{ServerExecutor: func(f func()) { f() }, ClientExecutor: func(f func()) { f() }})
	defer engine.Cancel()
	if engine.MaxHTTPHeaderSize != DefaultHTTPHeaderSize {
		t.Fatalf("MaxHTTPHeaderSize = %d, want %d", engine.MaxHTTPHeaderSize, DefaultHTTPHeaderSize)
	}
}

// TestHeaderBudgetBoundary checks exact request and response limits with whole and fragmented input.
func TestHeaderBudgetBoundary(t *testing.T) {
	for _, client := range []bool{false, true} {
		wire := "GET / HTTP/1.1\r\nHost: test\r\n\r\n"
		if client {
			wire = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
		}
		for _, delta := range []int{-1, 0, 1} {
			for _, block := range []int{1, 7, len(wire)} {
				t.Run(fmt.Sprintf("client=%v/delta=%d/block=%d", client, delta, block), func(t *testing.T) {
					processor := &headerBudgetProcessor{}
					parser := NewParser(nil, &Engine{Config: Config{MaxHTTPHeaderSize: len(wire) + delta}}, processor, client, nil)
					defer parser.CloseAndClean(nil)
					var err error
					for offset := 0; offset < len(wire); offset += block {
						end := offset + block
						if end > len(wire) {
							end = len(wire)
						}
						if err = parser.Parse([]byte(wire[offset:end])); err != nil {
							break
						}
					}
					want := 1
					if delta < 0 {
						want = 0
					}
					if errors.Is(err, ErrTooLong) != (delta < 0) || (delta >= 0 && err != nil) || processor.completed != want {
						t.Fatalf("error=%v completed=%d want=%d", err, processor.completed, want)
					}
				})
			}
		}
	}
}

// TestTrailerHeaderBudgetBoundary counts the final CRLF with and without trailer
// fields. Chunk framing and payload stay outside the budget, which resets after
// each complete message even when the next message arrives in the same read.
func TestTrailerHeaderBudgetBoundary(t *testing.T) {
	for _, client := range []bool{false, true} {
		for _, trailer := range []bool{false, true} {
			header := "POST / HTTP/1.1\r\nHost: test\r\n"
			if client {
				header = "HTTP/1.1 200 OK\r\n"
			}
			header += "Transfer-Encoding: chunked\r\n"
			tail := "\r\n"
			if trailer {
				header += "Trailer: X-Checksum\r\n"
				tail = "X-Checksum: valid\r\n\r\n"
			}
			header += "\r\n"
			budget := len(header) + len(tail)
			wire := header + "1\r\na\r\n0\r\n" + tail
			for _, delta := range []int{-2, -1, 0, 1} {
				for _, block := range []int{1, 7, 2 * len(wire)} {
					t.Run(fmt.Sprintf("client=%v/trailer=%v/delta=%d/block=%d", client, trailer, delta, block), func(t *testing.T) {
						processor := &headerBudgetProcessor{}
						parser := NewParser(nil, &Engine{Config: Config{MaxHTTPHeaderSize: budget + delta}}, processor, client, nil)
						defer parser.CloseAndClean(nil)
						data := []byte(wire + wire)
						var err error
						for offset := 0; offset < len(data); offset += block {
							end := offset + block
							if end > len(data) {
								end = len(data)
							}
							if err = parser.Parse(data[offset:end]); err != nil {
								break
							}
						}
						if delta < 0 {
							if !errors.Is(err, ErrTooLong) || processor.completed != 0 {
								t.Fatalf("error=%v completed=%d, want ErrTooLong before completion", err, processor.completed)
							}
						} else if err != nil || processor.completed != 2 {
							t.Fatalf("error=%v completed=%d, want two complete messages", err, processor.completed)
						}
					})
				}
			}
		}
	}
}

// TestHeaderBudgetClosesBeforeHandler checks that oversized unfinished headers
// close a real TCP connection before the handler can run.
func TestHeaderBudgetClosesBeforeHandler(t *testing.T) {
	for _, mode := range []int{IOModNonBlocking, IOModBlocking} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			var calls int32
			closed := make(chan error, 1)
			engine := NewEngine(Config{
				Addrs: []string{"127.0.0.1:0"}, NPoller: 1, IOMod: mode,
				MaxHTTPHeaderSize: 256, ReadBufferSize: 32, BlockingReadBufferSize: 32,
				Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { atomic.AddInt32(&calls, 1) }),
			})
			engine.OnClose(func(_ net.Conn, err error) { closed <- err })
			if err := engine.Start(); err != nil {
				t.Fatal(err)
			}
			defer engine.Stop()
			conn, err := net.Dial("tcp", engine.Addrs[0])
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			// Omit the terminating blank line so only the parser can enforce the limit.
			wire := "GET / HTTP/1.1\r\n" + strings.Repeat("X: y\r\n", 50)
			for offset := 0; offset < len(wire); offset += 8 {
				end := offset + 8
				if end > len(wire) {
					end = len(wire)
				}
				if _, err := conn.Write([]byte(wire[offset:end])); err != nil {
					break
				}
			}
			select {
			case err := <-closed:
				if !errors.Is(err, ErrTooLong) {
					t.Fatalf("close error=%v, want ErrTooLong", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("oversized unfinished headers did not close connection")
			}
			// The close callback alone does not prove that the transport was released.
			var response [1]byte
			if n, err := conn.Read(response[:]); err == nil || n != 0 {
				t.Fatalf("read after rejection = (%d, %v), want a closed connection", n, err)
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatalf("connection remained open after rejection: %v", err)
			}
			if calls := atomic.LoadInt32(&calls); calls != 0 {
				t.Fatalf("handler called %d times", calls)
			}
		})
	}
}
