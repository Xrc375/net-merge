package engine

import (
	"context"
	"net"
	"time"

	"net-merge/internal/nics"
)

// Internet reachability probes: concurrent TCP connects to well-known anycast IPs, bound to the NIC.
var probeTargets = [2][]string{
	Fam4: {"1.1.1.1:443", "8.8.8.8:443"},
	Fam6: {"[2606:4700:4700::1111]:443", "[2001:4860:4860::8888]:443"},
}

const (
	probeTimeout   = 1500 * time.Millisecond
	probeEveryUp   = 10 * time.Second
	probeEveryDown = 2 * time.Second
)

func probe(sl *Slot, f Family) bool {
	ip := sl.ip(f)
	if ip == nil {
		return false
	}
	network := "tcp4"
	if f == Fam6 {
		network = "tcp6"
	}
	targets := probeTargets[f]
	if len(targets) == 0 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	res := make(chan bool, len(targets))
	for _, t := range targets {
		go func(target string) {
			d := net.Dialer{
				LocalAddr: &net.TCPAddr{IP: ip},
				Control:   nics.ForceEgress(sl.Index, f == Fam6),
				Timeout:   probeTimeout,
			}
			if c, err := d.DialContext(ctx, network, target); err == nil {
				c.Close()
				res <- true
				return
			}
			res <- false
		}(t)
	}

	for range targets {
		if <-res {
			cancel()
			return true
		}
	}
	return false
}

type probeJob struct {
	sl  *Slot
	f   Family
	gen uint64
}

// TriggerProbe immediately probes a slot in the background (e.g. after a dial timeout)
// so a blackholed NIC is sidelined within ~1.5s instead of waiting for the periodic tick.
// If a probe is already in flight, it sets probePending so the latest generation is probed
// immediately upon completion without dropping the event.
func (s *Scheduler) TriggerProbe(sl *Slot, f Family) {
	if sl == nil {
		return
	}
	s.mu.Lock()
	if !sl.present || sl.ip(f) == nil {
		s.mu.Unlock()
		return
	}
	if sl.probing[f] {
		sl.probePending[f] = true
		s.mu.Unlock()
		return
	}
	sl.probing[f] = true
	sl.probePending[f] = false
	sl.lastProbe[f] = s.now()
	gen := sl.probeGen[f]
	s.mu.Unlock()

	go s.runProbeLoop(sl, f, gen)
}

func (s *Scheduler) runProbeLoop(sl *Slot, f Family, gen uint64) {
	for {
		up := probe(sl, f)
		s.mu.Lock()
		if sl.probeGen[f] == gen {
			s.applyStateLocked(sl, f, up)
		}
		if !sl.probePending[f] || !sl.present || sl.ip(f) == nil {
			sl.probing[f] = false
			sl.probePending[f] = false
			s.mu.Unlock()
			return
		}
		sl.probePending[f] = false
		sl.lastProbe[f] = s.now()
		gen = sl.probeGen[f]
		s.mu.Unlock()
	}
}

// RunProbes checks every adapter's internet access per address family: new ones immediately,
// healthy ones every 10s, dead ones every 2s so they rejoin quickly. Runs until stop is closed.
func (s *Scheduler) RunProbes(stop <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		for _, j := range s.probeDue() {
			go s.runProbeLoop(j.sl, j.f, j.gen)
		}
	}
}

// probeDue returns the (slot, family) pairs that need a probe now and marks them in flight.
func (s *Scheduler) probeDue() []probeJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var due []probeJob
	for _, sl := range s.slots {
		if !sl.present {
			continue
		}
		for _, f := range []Family{Fam4, Fam6} {
			if sl.ip(f) == nil || sl.probing[f] {
				continue
			}
			every := probeEveryUp
			switch sl.state[f] {
			case stateUnknown:
				every = 0
			case stateDown:
				every = probeEveryDown
			}
			if now.Sub(sl.lastProbe[f]) < every {
				continue
			}
			sl.probing[f] = true
			sl.probePending[f] = false
			sl.lastProbe[f] = now
			due = append(due, probeJob{sl, f, sl.probeGen[f]})
		}
	}
	return due
}