package probes

import (
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestNewHTTPServerSetsTimeouts(t *testing.T) {
	srv := newHTTPServer("127.0.0.1:0", http.NewServeMux())

	if srv.ReadHeaderTimeout != 5*time.Second {
		t.Errorf("ReadHeaderTimeout = %s, want 5s", srv.ReadHeaderTimeout)
	}
	for name, got := range map[string]time.Duration{
		"ReadTimeout":  srv.ReadTimeout,
		"WriteTimeout": srv.WriteTimeout,
		"IdleTimeout":  srv.IdleTimeout,
	} {
		if got <= 0 {
			t.Errorf("%s = %s, want > 0", name, got)
		}
	}
}

// A client that opens a connection and never finishes its request
// headers must be dropped once ReadHeaderTimeout passes, while a normal
// probe on the same server still answers.
func TestProbeServerDropsStalledClient(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := newHTTPServer(ln.Addr().String(), New("", nil).handler())
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", resp.StatusCode)
	}

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("GET /healthz HTTP/1.1\r\nHost: probe\r\n")); err != nil {
		t.Fatalf("write partial headers: %v", err)
	}

	start := time.Now()
	if err := conn.SetReadDeadline(start.Add(srv.ReadHeaderTimeout + 3*time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	_, err = conn.Read(make([]byte, 64))
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("stalled connection still open after %s", time.Since(start).Round(time.Second))
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read on stalled connection = %v, want EOF", err)
	}
}
