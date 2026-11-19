package engine

import (
	"net"
	"testing"
	"time"

	"net-merge/internal/nics"
)

func TestEcoModeSleepAndWakeOnTraffic(t *testing.T) {
	now := time.Now()
	s := NewScheduler([]nics.Nic{
		{Name: "EthPrimary", Index: 1, IP: net.ParseIP("10.0.0.1"), Weight: 1},
		{Name: "Usb5GSecondary", Index: 2, IP: net.ParseIP("10.0.0.2"), Weight: 1},
	})
	s.now = func() time.Time { return now }
	s.SetState(s.slots[0], Fam4, true)
	s.SetState(s.slots[1], Fam4, true)
	s.SetEcoMode(true)

	s.Tick(time.Second)
	now = now.Add(6 * time.Second)
	s.Tick(6 * time.Second)

	for i := 0; i < 10; i++ {
		sl := s.Acquire(nil)
		if sl.Name != "EthPrimary" {
			t.Fatalf("expected EthPrimary during eco consolidation, got %s", sl.Name)
		}
		s.Release(sl)
	}

	// Simulate > 5 Mbps burst to wake up secondary adapter.
	s.slots[0].bytesIn[Fam4].Add(2_000_000)
	now = now.Add(time.Second)
	s.Tick(time.Second)

	seen := false
	for i := 0; i < 20; i++ {
		sl := s.Acquire(nil)
		if sl.Name == "Usb5GSecondary" {
			seen = true
		}
		s.Release(sl)
	}
	if !seen {
		t.Fatalf("expected Usb5GSecondary to wake up under high traffic")
	}
}

func TestEcoModeSingleNic(t *testing.T) {
	now := time.Now()
	s := NewScheduler([]nics.Nic{
		{Name: "OnlyNic", Index: 1, IP: net.ParseIP("10.0.0.1"), Weight: 1},
	})
	s.now = func() time.Time { return now }
	s.SetState(s.slots[0], Fam4, true)
	s.SetEcoMode(true)

	s.Tick(time.Second)
	now = now.Add(6 * time.Second)
	s.Tick(6 * time.Second)

	sl := s.Acquire(nil)
	if sl == nil || sl.Name != "OnlyNic" {
		t.Fatalf("expected OnlyNic to remain usable, got %v", sl)
	}
	if sl.ecoSleep {
		t.Fatalf("single NIC must not be marked ecoSleep")
	}
	s.Release(sl)
}

func TestEcoModePrimaryDownFailover(t *testing.T) {
	now := time.Now()
	s := NewScheduler([]nics.Nic{
		{Name: "EthPrimary", Index: 1, IP: net.ParseIP("10.0.0.1"), Weight: 2},
		{Name: "Usb5GSecondary", Index: 2, IP: net.ParseIP("10.0.0.2"), Weight: 1},
	})
	s.now = func() time.Time { return now }
	s.SetState(s.slots[0], Fam4, false) // Primary is already down
	s.SetState(s.slots[1], Fam4, true)  // Secondary is up
	s.SetEcoMode(true)

	s.Tick(time.Second)
	now = now.Add(6 * time.Second)
	s.Tick(6 * time.Second)

	sl := s.Acquire(nil)
	if sl == nil || sl.Name != "Usb5GSecondary" {
		t.Fatalf("expected failover to Usb5GSecondary, got %v", sl)
	}
	if s.slots[1].ecoSleep {
		t.Fatalf("healthy secondary must not be deprioritized when primary is down")
	}
	s.Release(sl)
}

func TestEcoModeRuntimeFailover(t *testing.T) {
	now := time.Now()
	s := NewScheduler([]nics.Nic{
		{Name: "EthPrimary", Index: 1, IP: net.ParseIP("10.0.0.1"), Weight: 2},
		{Name: "Usb5GSecondary", Index: 2, IP: net.ParseIP("10.0.0.2"), Weight: 1},
	})
	s.now = func() time.Time { return now }
	s.SetState(s.slots[0], Fam4, true)
	s.SetState(s.slots[1], Fam4, true)
	s.SetEcoMode(true)

	// Enter Eco consolidation with both adapters up.
	s.Tick(time.Second)
	now = now.Add(6 * time.Second)
	s.Tick(6 * time.Second)

	if !s.slots[1].ecoSleep {
		t.Fatalf("expected Usb5GSecondary to be deprioritized while EthPrimary is up")
	}

	// Primary goes down at runtime.
	s.SetState(s.slots[0], Fam4, false)
	now = now.Add(time.Second)
	s.Tick(time.Second)

	if s.slots[1].ecoSleep {
		t.Fatalf("expected Usb5GSecondary to clear ecoSleep after EthPrimary goes down")
	}
	sl := s.Acquire(nil)
	if sl == nil || sl.Name != "Usb5GSecondary" {
		t.Fatalf("expected runtime failover to Usb5GSecondary, got %v", sl)
	}
	s.Release(sl)
}

func TestEcoModeWithGameMode(t *testing.T) {
	now := time.Now()
	s := NewScheduler([]nics.Nic{
		{Name: "EthPrimary", Index: 1, IP: net.ParseIP("10.0.0.1"), Weight: 1},
		{Name: "Usb5GSecondary", Index: 2, IP: net.ParseIP("10.0.0.2"), Weight: 1},
	})
	s.now = func() time.Time { return now }
	s.SetState(s.slots[0], Fam4, true)
	s.SetState(s.slots[1], Fam4, true)
	s.SetGameMode(true)
	s.SetEcoMode(true)

	// Enter Eco consolidation during idle period.
	s.Tick(time.Second)
	now = now.Add(6 * time.Second)
	s.Tick(6 * time.Second)

	if !s.slots[1].ecoSleep {
		t.Fatalf("expected Usb5GSecondary to enter ecoSleep before game activity")
	}

	// Trigger real low-bandwidth game UDP activity through Tick().
	sl0 := s.slots[0]
	u := s.trackUDP(sl0, Fam4)
	for i := 0; i < 12; i++ {
		now = now.Add(time.Second)
		for p := 0; p < 20; p++ {
			u.record(120, &net.UDPAddr{IP: net.ParseIP("1.1.1.1"), Port: 27015}, now)
		}
		s.Tick(time.Second)
	}

	if !sl0.gameActive {
		t.Fatalf("expected EthPrimary to become gameActive via Tick")
	}
	if s.slots[1].ecoSleep {
		t.Fatalf("expected game activity detected in Tick to clear ecoSleep")
	}
}
