// Package priority carries a request's criticality across hops and holds
// the DAGOR-style admission level that sheds the least important requests
// first when a server is overloaded.
package priority

import (
	"context"
	"hash/fnv"
	"strconv"
)

// Tier is a request's business criticality. Lower is more important.
type Tier int

const (
	Critical  Tier = 0 // user-visible actions that must not fail (book, pay, log in)
	Default   Tier = 1 // ordinary interactive traffic
	Sheddable Tier = 2 // prefetch, recommendations, anything that can be dropped
	NumTiers       = 3
)

// Header is the metadata key (gRPC) and HTTP header carrying the tier.
const Header = "x-lc-tier"

// UserHeader carries a stable caller identity used for the DAGOR user
// priority. If absent the request id or nothing is used.
const UserHeader = "x-lc-user"

func (t Tier) String() string {
	switch t {
	case Critical:
		return "critical"
	case Default:
		return "default"
	case Sheddable:
		return "sheddable"
	}
	return "tier" + strconv.Itoa(int(t))
}

// Parse reads a tier from a header value. Names and digits are accepted;
// anything else is Default.
func Parse(s string) Tier {
	switch s {
	case "critical", "0":
		return Critical
	case "default", "1", "":
		return Default
	case "sheddable", "2":
		return Sheddable
	}
	if n, err := strconv.Atoi(s); err == nil && n >= 0 && n < 64 {
		return Tier(n)
	}
	return Default
}

type ctxKey struct{}

// Info is the priority a request carries: its tier and its user priority
// (0..UserLevels-1), which DAGOR uses to shed whole users at a time rather
// than random requests, so a user who gets in tends to finish the session.
type Info struct {
	Tier Tier
	User int
}

// UserLevels is the number of user-priority buckets, as in DAGOR.
const UserLevels = 128

// WithInfo returns ctx carrying p.
func WithInfo(ctx context.Context, p Info) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext returns the priority on ctx, or Default with user 0.
func FromContext(ctx context.Context) (Info, bool) {
	p, ok := ctx.Value(ctxKey{}).(Info)
	if !ok {
		return Info{Tier: Default}, false
	}
	return p, true
}

// UserPriority hashes a caller identity to a user priority. DAGOR rotates
// the hash periodically so the same users are not always the ones shed;
// epoch is that rotation counter.
func UserPriority(user string, epoch uint64) int {
	if user == "" {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(user))
	var b [8]byte
	for i := range b {
		b[i] = byte(epoch >> (8 * i))
	}
	h.Write(b[:])
	return int(h.Sum32() % UserLevels)
}
