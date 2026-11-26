package engine

import (
	"fmt"
	"net"
	"testing"
	"time"

	"net-merge/internal/nics"
)

func TestStickyCacheSmartMatchingAndTTL(t *testing.T) {
	sc := NewStickyCache(10 * time.Minute)
	now := time.Date(2026, 11, 26, 18, 0, 0, 0, time.UTC)
	sc.now = func() time.Time { return now }

	mustStick := []string{
		"login.microsoftonline.com",
		"ebank.esunbank.com.tw",
		"id.apple.com",
		"portal.id.example.com",
		"www.gov.tw",
	}
	for _, h := range mustStick {
		if !sc.ShouldStick(h) {
			t.Fatalf("expected %q to be sticky in smart mode", h)
		}
	}

	mustNotStick := []string{
		"cdn.steampowered.com",
		"android.googleapis.com",
		"vid.cloudfront.net",
		"rapid.cdn.example.com",
	}
	for _, h := range mustNotStick {
		if sc.ShouldStick(h) {
			t.Fatalf("expected %q NOT to be sticky in smart mode", h)
		}
	}

	sc.Set("login.microsoftonline.com", "Ethernet")
	if got, ok := sc.Get("login.microsoftonline.com"); !ok || got != "Ethernet" {
		t.Fatalf("expected Ethernet, got %q (ok=%v)", got, ok)
	}

	now = now.Add(11 * time.Minute)
	if _, ok := sc.Get("login.microsoftonline.com"); ok {
		t.Fatal("expected entry to expire after TTL")
	}
}

func TestStickyCacheHardCap(t *testing.T) {
	sc := NewStickyCache(15 * time.Minute)
	sc.maxSize = 64

	for i := 0; i < 200; i++ {
		sc.Set(fmt.Sprintf("10.0.0.%d", i), "Ethernet")
		if len(sc.entries) > sc.maxSize {
			t.Fatalf("cache exceeded maxSize %d: got %d", sc.maxSize, len(sc.entries))
		}
	}
}

func TestAcquirePreferredAndFailover(t *testing.T) {
	s := NewScheduler([]nics.Nic{
		{Name: "Ethernet", Index: 1, IP: net.ParseIP("192.168.1.10"), Weight: 1},
		{Name: "Wi-Fi", Index: 2, IP: net.ParseIP("192.168.2.10"), Weight: 1},
	})
	for _, sl := range s.slots {
		s.SetState(sl, Fam4, true)
	}

	for i := 0; i < 5; i++ {
		sl := s.AcquirePreferred("Ethernet", Fam4, nil)
		if sl.key != "Ethernet" {
			t.Fatalf("iter %d: expected preferred Ethernet, got %s", i, sl.key)
		}
		s.ReleaseFor(sl, Fam4)
	}

	s.SetState(s.slots[0], Fam4, false)
	sl := s.AcquirePreferred("Ethernet", Fam4, nil)
	if sl.key != "Wi-Fi" {
		t.Fatalf("expected failover to Wi-Fi when Ethernet is down, got %s", sl.key)
	}
	s.ReleaseFor(sl, Fam4)
}