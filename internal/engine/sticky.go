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
	mu        sync.RWMutex
	ttl       time.Duration
	entries   map[string]stickyEntry
	now       func() time.Time
	mode      string // "smart" (default), "all", "off"
	patterns  []string
	maxSize   int
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
			if strings.Contains(h, p) {
				return true
			}
		}
		return false
	}
}

func (c *StickyCache) Get(host string) (string, bool) {
	c.mu.RLock()
	e, ok := c.entries[host]
	now := c.now()
	c.mu.RUnlock()
	if !ok || now.After(e.expiresAt) {
		return "", false
	}
	return e.slotKey, true
}

func (c *StickyCache) Set(host, slotKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if len(c.entries) >= c.maxSize {
		for k, e := range c.entries {
			if now.After(e.expiresAt) {
				delete(c.entries, k)
			}
		}
	}
	c.entries[host] = stickyEntry{
		slotKey:   slotKey,
		expiresAt: now.Add(c.ttl),
	}
}

func (c *StickyCache) Delete(host string) {
	c.mu.Lock()
	delete(c.entries, host)
	c.mu.Unlock()
}