package engine

import (
	"net"
	"testing"
	"time"

	"net-merge/internal/nics"
)

func TestStickyCacheSmartMatchingAndTTL(t *testing.T) {
	sc := NewStickyCache(10 * time.Minute)
	now := time.Date(2026, 11, 26, 18, 0, 0, 0, time.UTC)
	sc.now = func() time.Time { return now }

	if !sc.ShouldStick("login.microsoftonline.com") || !sc.ShouldStick("ebank.esunbank.com.tw") {
		t.Fatal("expected auth/banking domains to be sticky in smart mode")
	}
	if sc.ShouldStick("cdn.steampowered.com") {
		t.Fatal("expected bulk CDN domain NOT to be sticky in smart mode")
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

func TestAcquirePreferredAndFailover(t *testing.T) {
	s := NewScheduler([]nics.Nic{
		{Name: "Ethernet", Index: 1, IP: net.ParseIP("192.168.1.10"), Weight: 1},
		{Name: "Wi-Fi", Index: 2, IP: net.ParseIP("192.168.2.10"), Weight: 1},
	})
	for _, sl := range s.slots {
		s.SetState(sl, Fam4, true)
	}

	// 即使反覆請求，只要 Ethernet 健康就應持續命中
	for i := 0; i < 5; i++ {
		sl := s.AcquirePreferred("Ethernet", Fam4, nil)
		if sl.key != "Ethernet" {
			t.Fatalf("iter %d: expected preferred Ethernet, got %s", i, sl.key)
		}
		s.ReleaseFor(sl, Fam4)
	}

	// 模擬 Ethernet 斷線 -> 必須自動轉移到 Wi-Fi
	s.SetState(s.slots[0], Fam4, false)
	sl := s.AcquirePreferred("Ethernet", Fam4, nil)
	if sl.key != "Wi-Fi" {
		t.Fatalf("expected failover to Wi-Fi when Ethernet is down, got %s", sl.key)
	}
	s.ReleaseFor(sl, Fam4)
}