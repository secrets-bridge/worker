package sweepers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/secrets-bridge/api/pkg/runtime"

	"github.com/secrets-bridge/worker/internal/notifications"
)

// UsersPending tells the admins when accounts start waiting for
// approval (worker#34). Someone who signs in with Google for the first
// time gets an account that can't do anything until an admin approves
// it on Admin → Users (api#265). Each run posts at most one message:
// the accounts that started waiting since the last message, up to
// UsersPendingListMax by name and email, then "and N more", and a link
// to Admin → Users built from the install's public address.
//
// The message names people, so it goes to the webhook only. Main never
// hands this sweeper the log sink, and the sweeper itself logs counts
// only. Without a webhook it isn't registered: the console's badge and
// Overview banner already show who is waiting.
//
// The cursor (created_at and id of the newest account announced) lives
// in Redis and moves only after the webhook accepts the message, so a
// failed post is retried on the next run, together with anyone who
// arrived since. With no cursor (first run, or Redis lost it), the
// accounts already waiting are announced once.
//
// created_at is the inserting transaction's start time, so an account
// whose transaction commits late can carry a time older than one that
// was already announced. Accounts younger than UsersPendingSettle are
// left for the next run, so they can't slip behind the cursor.
type UsersPending struct {
	Pool     pgxRowQuerier
	Cursor   usersPendingCursorStore
	Notifier notifications.Notifier
	// PublicURL is SB_PUBLIC_URL, without a trailing slash. When set it
	// wins over the address saved in the console, as in the api.
	PublicURL string
}

const (
	// UsersPendingListMax is how many accounts one message names.
	UsersPendingListMax = 10
	// UsersPendingSettle is how old an account must be to be announced.
	UsersPendingSettle = 30 * time.Second

	maxNameRunes  = 80
	maxEmailRunes = 254
)

// UsersPendingCursor is the newest account a message announced.
type UsersPendingCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type usersPendingCursorStore interface {
	// Get returns the cursor, or ok=false when there is none yet.
	Get(ctx context.Context) (c UsersPendingCursor, ok bool, err error)
	Set(ctx context.Context, c UsersPendingCursor) error
}

// Name returns the stable identifier for this sweeper.
func (s UsersPending) Name() string { return "users-pending" }

type pendingAccount struct {
	email, name string
}

// Run announces the accounts that started waiting since the last
// message. A failed post leaves the cursor where it was and isn't an
// error: the next tick retries it.
func (s UsersPending) Run(ctx context.Context) error {
	cur, _, err := s.Cursor.Get(ctx)
	if err != nil {
		return fmt.Errorf("sweepers: users-pending: read cursor: %w", err)
	}
	// The zero cursor (year 1, nil id) sorts before every account.
	const q = `
		SELECT email, display_name,
		       count(*) OVER (),
		       first_value(created_at) OVER newest,
		       first_value(id) OVER newest,
		       (SELECT count(*) FROM local_users WHERE pending_approval AND NOT disabled),
		       (SELECT COALESCE(public_url, '') FROM sso_settings WHERE id)
		FROM local_users
		WHERE pending_approval AND NOT disabled
		  AND (created_at, id) > ($1::timestamptz, $2::uuid)
		  AND created_at <= now() - make_interval(secs => $3)
		WINDOW newest AS (ORDER BY created_at DESC, id DESC)
		ORDER BY created_at, id
		LIMIT $4`
	rows, err := s.Pool.Query(ctx, q, cur.CreatedAt, cur.ID, UsersPendingSettle.Seconds(), UsersPendingListMax)
	if err != nil {
		return fmt.Errorf("sweepers: users-pending: list: %w", err)
	}
	defer rows.Close()
	var (
		accounts         []pendingAccount
		fresh, waiting   int64
		newest           UsersPendingCursor
		consolePublicURL string
	)
	for rows.Next() {
		var a pendingAccount
		if err := rows.Scan(&a.email, &a.name, &fresh, &newest.CreatedAt, &newest.ID, &waiting, &consolePublicURL); err != nil {
			return fmt.Errorf("sweepers: users-pending: scan: %w", err)
		}
		accounts = append(accounts, a)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sweepers: users-pending: list: %w", err)
	}
	if len(accounts) == 0 {
		return nil
	}

	publicURL := s.PublicURL
	if publicURL == "" {
		publicURL = strings.TrimRight(consolePublicURL, "/")
	}
	ev := usersPendingEvent(accounts, fresh, waiting, publicURL)
	if !notify(ctx, s.Notifier, ev) {
		return nil
	}
	if err := s.Cursor.Set(ctx, newest); err != nil {
		// The next run announces these accounts again; better twice
		// than never.
		return fmt.Errorf("sweepers: users-pending: save cursor: %w", err)
	}
	slog.Default().Info("sweepers: announced waiting accounts",
		"sweeper", s.Name(), "new", fresh, "waiting", waiting)
	return nil
}

func usersPendingEvent(accounts []pendingAccount, fresh, waiting int64, publicURL string) notifications.Event {
	title := fmt.Sprintf("%d new accounts are waiting for approval", fresh)
	if fresh == 1 {
		title = "1 new account is waiting for approval"
	}
	var b strings.Builder
	for _, a := range accounts {
		email := plainText(a.email, maxEmailRunes)
		if name := plainText(a.name, maxNameRunes); name != "" && name != email {
			fmt.Fprintf(&b, "• %s (%s)\n", name, email)
		} else {
			fmt.Fprintf(&b, "• %s\n", email)
		}
	}
	if more := fresh - int64(len(accounts)); more > 0 {
		fmt.Fprintf(&b, "and %d more.\n", more)
	}
	if waiting > fresh {
		fmt.Fprintf(&b, "%d waiting in all. ", waiting)
	}
	meta := map[string]any{"new": fresh, "waiting": waiting}
	if publicURL != "" {
		link := publicURL + "/admin/users"
		fmt.Fprintf(&b, "Approve or decline them in Admin → Users: %s", link)
		meta["link"] = link
	} else {
		b.WriteString("Approve or decline them in the console, under Admin → Users.")
	}
	return notifications.Event{
		Severity:  notifications.SeverityInfo,
		Component: "users-pending",
		Title:     title,
		Detail:    b.String(),
		Metadata:  meta,
	}
}

// plainText makes a name or email safe to put on one line of a chat
// message. Display names come from the person's Google account, so they
// can hold anything. It drops control and format characters (newlines,
// bidi overrides, zero-width marks) and invalid UTF-8, folds whitespace
// runs into one space and cuts the text at max runes with "…". The
// Slack format escapes &, < and > on top.
func plainText(s string, maxRunes int) string {
	var out []rune
	space := false
	for _, r := range s {
		switch {
		case r == utf8.RuneError, unicode.Is(unicode.Cf, r):
			continue
		case unicode.IsSpace(r) || unicode.IsControl(r):
			space = len(out) > 0
			continue
		}
		if space {
			out = append(out, ' ')
			space = false
		}
		out = append(out, r)
	}
	if len(out) > maxRunes {
		return strings.TrimRight(string(out[:maxRunes-1]), " ") + "…"
	}
	return string(out)
}

// RedisUsersPendingCursor keeps the users-pending cursor in Redis, as
// "<RFC 3339 time> <id>".
type RedisUsersPendingCursor struct {
	Client *runtime.Client
}

func (c RedisUsersPendingCursor) key() string { return c.Client.Key("cursor", "users-pending") }

// Get returns the stored cursor. A missing or unreadable value counts
// as no cursor, so the accounts already waiting are announced once
// instead of the sweeper failing on every run.
func (c RedisUsersPendingCursor) Get(ctx context.Context) (UsersPendingCursor, bool, error) {
	v, err := c.Client.Raw().Get(ctx, c.key()).Result()
	if errors.Is(err, redis.Nil) {
		return UsersPendingCursor{}, false, nil
	}
	if err != nil {
		return UsersPendingCursor{}, false, err
	}
	at, id, found := strings.Cut(v, " ")
	t, terr := time.Parse(time.RFC3339Nano, at)
	u, uerr := uuid.Parse(id)
	if !found || terr != nil || uerr != nil {
		return UsersPendingCursor{}, false, nil
	}
	return UsersPendingCursor{CreatedAt: t, ID: u}, true, nil
}

// Set stores the cursor, with no expiry.
func (c RedisUsersPendingCursor) Set(ctx context.Context, cur UsersPendingCursor) error {
	return c.Client.Raw().Set(ctx, c.key(), cur.CreatedAt.UTC().Format(time.RFC3339Nano)+" "+cur.ID.String(), 0).Err()
}
