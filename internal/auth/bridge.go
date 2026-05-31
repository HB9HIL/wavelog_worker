package auth

import (
	"regexp"
	"strconv"

	wlhmac "github.com/wavelog/wavelog_worker/internal/hmac"
	"github.com/wavelog/wavelog_worker/internal/registry"
)

var reSession = regexp.MustCompile(`^session\.(\d+)$`)

type Bridge struct {
	reg    *registry.Registry
	secret string
}

func NewBridge(reg *registry.Registry, secret string) *Bridge {
	return &Bridge{reg: reg, secret: secret}
}

// Validate checks that the topic is registered and the HMAC token is valid.
// Returns false for unknown topics — PHP must register before browsers can connect.
func (b *Bridge) Validate(topic, token string) bool {
	meta, ok := b.reg.Lookup(topic)
	if !ok {
		return false
	}
	if !meta.RequireToken {
		return true
	}
	m := reSession.FindStringSubmatch(topic)
	if m == nil {
		return false
	}
	sessionID, _ := strconv.Atoi(m[1])

	claims, err := wlhmac.Verify(token, b.secret)
	if err != nil {
		return false
	}
	return claims.SessionID == sessionID
}
