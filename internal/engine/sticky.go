package engine

import (
	"os"
	"strings"
	"sync"
	"time"
)

type stickyEntry struct {
	slotKey   string
	expiresAt time.Time
}

// StickyCache keeps per-host NIC affinity with TTL to prevent IP drifting on auth/banking sessions.
type StickyCache struct {
	mu       sync.RWMutex
	ttl      time.Duration
	entries  map[string]stickyEntry
	now      func() time.Time
	mode     string // "smart" (default), "all", "off"
	patterns []string
	maxSize  int
}

var defaultStickyPatterns = []string{
	"bank", "login", "auth", "sso", "oauth", "signin",
	"account", "paypal", "stripe", "gov.", "id.",
}

func NewStickyCache(ttl time.Duration) *StickyCache {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("NETMERGE_STICKY_MODE")))
	if mode == "" {
		mode = "smart"
	}
	patterns := append([]string(nil), defaultStickyPatterns...)
	if extra := os.Getenv("NETMERGE_STICKY_DOMAINS"); extra != "" {
		for _, p := range strings.Split(extra, ",") {
			if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
				patterns = append(patterns, p)
			}
		}
	}
	return &StickyCache{
		ttl:      ttl,
		entries:  make(map[string]stickyEntry),
		now:      time.Now,
		mode:     mode,
		patterns: patterns,
		maxSize:  4096,
	}
}

func (c *StickyCache) SetMode(mode string) {
	c.mu.Lock()
	c.mode = strings.ToLower(strings.TrimSpace(mode))
	c.mu.Unlock()
}

func matchStickyPattern(host, pattern string) bool {
	// Patterns ending with "." (e.g. "id.", "gov.") must match on a label boundary
	// so "id." matches "id.apple.com" without false-matching "android.googleapis.com".
	if strings.HasSuffix(pattern, ".") {
		return strings.HasPrefix(host, pattern) || strings.Contains(host, "."+pattern)
	}
	return strings.Contains(host, pattern)
}

// ShouldStick decides whether a destination host should pin to a single egress NIC.
func (c *StickyCache) ShouldStick(host string) bool {
	c.mu.RLock()
	mode := c.mode
	patterns := c.patterns
	c.mu.RUnlock()

	switch mode {
	case "off":
		return false
	case "all":
		return host != ""
	default: // "smart"
		h := strings.ToLower(host)
		for _, p := range patterns {
			if matchStickyPattern(h, p) {
				return true
			}
		}
		return false
	}
}

func (c *StickyCache) Get(host string) (string, bool) {
	h := strings.ToLower(strings.TrimSpace(host))
	c.mu.RLock()
	e, ok := c.entries[h]
	now := c.now()
	c.mu.RUnlock()
	if !ok || now.After(e.expiresAt) {
		return "", false
	}
	return e.slotKey, true
}

func (c *StickyCache) Set(host, slotKey string) {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if _, exists := c.entries[h]; !exists && c.maxSize > 0 && len(c.entries) >= c.maxSize {
		for k, e := range c.entries {
			if now.After(e.expiresAt) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.maxSize {
			evict := c.maxSize / 4
			if evict < 1 {
				evict = 1
			}
			for k := range c.entries {
				delete(c.entries, k)
				evict--
				if evict <= 0 {
					break
				}
			}
		}
	}
	c.entries[h] = stickyEntry{
		slotKey:   slotKey,
		expiresAt: now.Add(c.ttl),
	}
}

func (c *StickyCache) Delete(host string) {
	h := strings.ToLower(strings.TrimSpace(host))
	c.mu.Lock()
	delete(c.entries, h)
	c.mu.Unlock()
}

// DeleteIf removes the sticky binding for host only if it still points to expectedSlotKey,
// preventing an older failed dial from erasing a newer successful binding on another NIC.
func (c *StickyCache) DeleteIf(host, expectedSlotKey string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[h]
	if !ok || entry.slotKey != expectedSlotKey {
		return false
	}
	delete(c.entries, h)
	return true
}