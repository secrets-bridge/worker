package sweepers_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/secrets-bridge/api/pkg/runtime"
	"github.com/secrets-bridge/api/pkg/storage"

	"github.com/secrets-bridge/worker/internal/notifications"
	"github.com/secrets-bridge/worker/internal/sweepers"
)

// memCursor is an in-memory cursor store.
type memCursor struct {
	cur  sweepers.UsersPendingCursor
	ok   bool
	sets int
}

func (m *memCursor) Get(context.Context) (sweepers.UsersPendingCursor, bool, error) {
	return m.cur, m.ok, nil
}

func (m *memCursor) Set(_ context.Context, c sweepers.UsersPendingCursor) error {
	m.cur, m.ok = c, true
	m.sets++
	return nil
}

// webhookSink records events, or fails like a webhook that's down.
type webhookSink struct {
	events []notifications.Event
	fail   bool
}

func (w *webhookSink) Notify(_ context.Context, e notifications.Event) error {
	if w.fail {
		return errors.New("webhook returned HTTP 503 (transient)")
	}
	w.events = append(w.events, e)
	return nil
}

func (w *webhookSink) Name() string { return "webhook" }

// pendingPool is testPool with the console's public address cleared.
func pendingPool(t *testing.T) *storage.Pool {
	t.Helper()
	pool := testPool(t)
	if _, err := pool.Exec(t.Context(), `UPDATE sso_settings SET public_url = NULL`); err != nil {
		t.Fatalf("reset sso_settings: %v", err)
	}
	return pool
}

type account struct {
	email, name      string
	age              time.Duration // how long ago it was created
	active, disabled bool          // active: not waiting for approval
}

func addAccounts(t *testing.T, pool *storage.Pool, accounts ...account) {
	t.Helper()
	for _, a := range accounts {
		if _, err := pool.Exec(t.Context(), `
			INSERT INTO local_users (email, password_hash, display_name, pending_approval, disabled, created_at)
			VALUES ($1, '\x00', $2, $3, $4, now() - make_interval(secs => $5))`,
			a.email, a.name, !a.active, a.disabled, a.age.Seconds()); err != nil {
			t.Fatalf("insert %s: %v", a.email, err)
		}
	}
}

func runPending(t *testing.T, s sweepers.UsersPending) {
	t.Helper()
	if err := s.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Announces each waiting account once, oldest first, and leaves out
// active and declined (disabled) accounts and ones younger than the
// settle time.
func TestUsersPending_AnnouncesNewAccountsOnce(t *testing.T) {
	pool := pendingPool(t)
	addAccounts(t, pool,
		account{email: "sara@example.com", name: "Sara Ahmed", age: 3 * time.Hour},
		account{email: "omar@example.com", age: 2 * time.Hour},
		account{email: "lina@example.com", name: "Lina Haddad", age: time.Hour},
		account{email: "declined@example.com", age: 2 * time.Hour, disabled: true},
		account{email: "active@example.com", age: 2 * time.Hour, active: true},
		account{email: "young@example.com", age: 5 * time.Second},
	)
	sink, cur := &webhookSink{}, &memCursor{}
	s := sweepers.UsersPending{Pool: pool, Cursor: cur, Notifier: sink}

	runPending(t, s)
	if len(sink.events) != 1 {
		t.Fatalf("got %d messages, want 1", len(sink.events))
	}
	ev := sink.events[0]
	if ev.Title != "3 new accounts are waiting for approval" {
		t.Errorf("title = %q", ev.Title)
	}
	wantList := "• Sara Ahmed (sara@example.com)\n• omar@example.com\n• Lina Haddad (lina@example.com)\n4 waiting in all. "
	if !strings.HasPrefix(ev.Detail, wantList) {
		t.Errorf("detail = %q, want it to start with %q", ev.Detail, wantList)
	}
	for _, left := range []string{"declined@", "active@", "young@"} {
		if strings.Contains(ev.Detail, left) {
			t.Errorf("detail names %s: %q", left, ev.Detail)
		}
	}
	if ev.Metadata["new"] != int64(3) || ev.Metadata["waiting"] != int64(4) {
		t.Errorf("metadata = %v", ev.Metadata)
	}

	runPending(t, s)
	if len(sink.events) != 1 {
		t.Fatalf("second run sent another message: %+v", sink.events[1:])
	}

	// The young account settles and is announced alone.
	if _, err := pool.Exec(t.Context(), `UPDATE local_users SET created_at = now() - interval '31 seconds' WHERE email = 'young@example.com'`); err != nil {
		t.Fatal(err)
	}
	runPending(t, s)
	if len(sink.events) != 2 {
		t.Fatalf("got %d messages, want 2", len(sink.events))
	}
	if ev := sink.events[1]; ev.Title != "1 new account is waiting for approval" || !strings.HasPrefix(ev.Detail, "• young@example.com\n4 waiting in all. ") {
		t.Errorf("third run: %q / %q", ev.Title, ev.Detail)
	}
}

// One message per run, however many arrive: ten by name, then a count.
// The cursor moves past all of them, so the rest aren't announced
// again.
func TestUsersPending_TenByNameThenACount(t *testing.T) {
	pool := pendingPool(t)
	for i := range 12 {
		addAccounts(t, pool, account{email: fmt.Sprintf("p%02d@example.com", i), age: time.Duration(20-i) * time.Minute})
	}
	sink := &webhookSink{}
	s := sweepers.UsersPending{Pool: pool, Cursor: &memCursor{}, Notifier: sink}

	runPending(t, s)
	if len(sink.events) != 1 {
		t.Fatalf("got %d messages, want 1", len(sink.events))
	}
	ev := sink.events[0]
	if ev.Title != "12 new accounts are waiting for approval" {
		t.Errorf("title = %q", ev.Title)
	}
	if n := strings.Count(ev.Detail, "• "); n != sweepers.UsersPendingListMax {
		t.Errorf("named %d accounts, want %d", n, sweepers.UsersPendingListMax)
	}
	if !strings.Contains(ev.Detail, "• p09@example.com\nand 2 more.\n") || strings.Contains(ev.Detail, "p10@") {
		t.Errorf("detail = %q", ev.Detail)
	}

	runPending(t, s)
	if len(sink.events) != 1 {
		t.Fatalf("the two not named were announced again: %+v", sink.events[1:])
	}
}

// A post the webhook doesn't accept leaves the cursor where it was, so
// the next run announces those accounts with anyone who came since.
func TestUsersPending_FailedPostIsRetried(t *testing.T) {
	pool := pendingPool(t)
	addAccounts(t, pool, account{email: "first@example.com", age: time.Hour})
	sink, cur := &webhookSink{fail: true}, &memCursor{}
	s := sweepers.UsersPending{Pool: pool, Cursor: cur, Notifier: sink}

	runPending(t, s)
	if cur.sets != 0 {
		t.Fatalf("cursor moved after a failed post: %+v", cur.cur)
	}

	addAccounts(t, pool, account{email: "second@example.com", age: time.Minute})
	sink.fail = false
	runPending(t, s)
	if len(sink.events) != 1 || !strings.HasPrefix(sink.events[0].Detail, "• first@example.com\n• second@example.com\n") {
		t.Fatalf("retry = %+v", sink.events)
	}
	if cur.sets != 1 {
		t.Fatalf("cursor set %d times, want 1", cur.sets)
	}
}

// The link uses SB_PUBLIC_URL, else the address saved in the console.
// With neither, the message says where to go without a link.
func TestUsersPending_LinkToAdminUsers(t *testing.T) {
	pool := pendingPool(t)
	addAccounts(t, pool, account{email: "sara@example.com", age: time.Hour})
	run := func(envURL string) notifications.Event {
		t.Helper()
		sink := &webhookSink{}
		runPending(t, sweepers.UsersPending{Pool: pool, Cursor: &memCursor{}, Notifier: sink, PublicURL: envURL})
		if len(sink.events) != 1 {
			t.Fatalf("got %d messages, want 1", len(sink.events))
		}
		return sink.events[0]
	}

	ev := run("")
	if !strings.HasSuffix(ev.Detail, "Approve or decline them in the console, under Admin → Users.") || ev.Metadata["link"] != nil {
		t.Errorf("no address: %q %v", ev.Detail, ev.Metadata)
	}

	if _, err := pool.Exec(t.Context(), `UPDATE sso_settings SET public_url = 'https://console.example.com'`); err != nil {
		t.Fatal(err)
	}
	ev = run("")
	if !strings.HasSuffix(ev.Detail, "Admin → Users: https://console.example.com/admin/users") || ev.Metadata["link"] != "https://console.example.com/admin/users" {
		t.Errorf("console address: %q %v", ev.Detail, ev.Metadata)
	}

	ev = run("https://env.example.com")
	if !strings.HasSuffix(ev.Detail, "https://env.example.com/admin/users") {
		t.Errorf("SB_PUBLIC_URL should win: %q", ev.Detail)
	}
}

// A display name comes from the person's Google account. In the Slack
// message it can't break onto new lines, reorder the text with bidi
// controls, mention @channel or hide a link, and a long one is cut.
func TestUsersPending_NamesAreCleaned(t *testing.T) {
	pool := pendingPool(t)
	addAccounts(t, pool,
		account{email: "eve@example.com", name: "Eve\n\n<!channel>\u202Eevil\u200B <https://phish.example|Approve>", age: 2 * time.Hour},
		account{email: "long@example.com", name: strings.Repeat("a", 200), age: time.Hour},
	)
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]string
		if err := json.Unmarshal(b, &m); err != nil {
			t.Errorf("slack body not JSON: %s", b)
		}
		body = m["text"]
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	webhook := &notifications.Webhook{URL: srv.URL, HTTPClient: srv.Client(), FormatSlack: true}

	runPending(t, sweepers.UsersPending{Pool: pool, Cursor: &memCursor{}, Notifier: webhook})

	wantEve := "• Eve &lt;!channel&gt;evil &lt;https://phish.example|Approve&gt; (eve@example.com)\n"
	if !strings.Contains(body, wantEve) {
		t.Errorf("slack text = %q, want it to contain %q", body, wantEve)
	}
	for _, bad := range []string{"<!channel>", "<https://", "\u202E", "\u200B", "Eve\n"} {
		if strings.Contains(body, bad) {
			t.Errorf("slack text contains %q: %q", bad, body)
		}
	}
	if want := "• " + strings.Repeat("a", 79) + "… (long@example.com)\n"; !strings.Contains(body, want) {
		t.Errorf("long name not cut at 80: %q", body)
	}
	if !strings.HasPrefix(body, "[info] 2 new accounts are waiting for approval\n• ") {
		t.Errorf("list doesn't start on its own line: %q", body)
	}
}

// Who is waiting goes to the webhook only. The worker's log gets the
// counts, whether the post works or fails.
func TestUsersPending_NeverLogsWhoIsWaiting(t *testing.T) {
	logs := captureDefaultLog(t)
	pool := pendingPool(t)
	addAccounts(t, pool, account{email: "sara@example.com", name: "Sara Ahmed", age: time.Hour})
	sink, cur := &webhookSink{fail: true}, &memCursor{}
	s := sweepers.UsersPending{Pool: pool, Cursor: cur, Notifier: sink}

	runPending(t, s)
	sink.fail = false
	runPending(t, s)
	if len(sink.events) != 1 {
		t.Fatalf("got %d messages, want 1", len(sink.events))
	}
	out := logs.String()
	if !strings.Contains(out, "notification failed") || !strings.Contains(out, "announced waiting accounts") {
		t.Fatalf("expected the failure and the success in the log: %s", out)
	}
	for _, who := range []string{"sara@example.com", "Sara Ahmed"} {
		if strings.Contains(out, who) {
			t.Errorf("log names %q: %s", who, out)
		}
	}
}

// The Redis cursor round-trips, and a missing or unreadable value counts
// as no cursor rather than an error that would stop every run.
func TestRedisUsersPendingCursor(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL not set; skipping Redis-backed cursor test")
	}
	rt, err := runtime.Open(t.Context(), runtime.Config{URL: url, Namespace: "sbtest-worker-" + uuid.NewString()[:8]})
	if err != nil {
		t.Fatalf("runtime.Open: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	c := sweepers.RedisUsersPendingCursor{Client: rt}
	key := rt.Key("cursor", "users-pending")
	t.Cleanup(func() { _ = rt.Raw().Del(context.Background(), key).Err() })

	if _, ok, err := c.Get(t.Context()); ok || err != nil {
		t.Fatalf("empty Get = ok %v, err %v", ok, err)
	}
	want := sweepers.UsersPendingCursor{CreatedAt: time.Date(2026, 10, 19, 8, 30, 1, 123456000, time.UTC), ID: uuid.New()}
	if err := c.Set(t.Context(), want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok, err := c.Get(t.Context())
	if err != nil || !ok || !got.CreatedAt.Equal(want.CreatedAt) || got.ID != want.ID {
		t.Fatalf("Get = %+v %v %v, want %+v", got, ok, err, want)
	}
	if err := rt.Raw().Set(t.Context(), key, "not a cursor", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.Get(t.Context()); ok || err != nil {
		t.Fatalf("unreadable Get = ok %v, err %v", ok, err)
	}
}
