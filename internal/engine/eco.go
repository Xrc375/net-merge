package engine

import "time"

const (
	// ecoLowTrafficThreshold is 5 Mbps in bytes/sec (5,000,000 / 8).
	ecoLowTrafficThreshold = 625_000
	// ecoIdleHoldDuration is how long total traffic must stay below the
	// threshold before secondary adapters are deprioritized.
	ecoIdleHoldDuration = 5 * time.Second
)

// SetEcoMode enables or disables adaptive traffic consolidation during low traffic.
func (s *Scheduler) SetEcoMode(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ecoMode = on
	if !on {
		s.ecoLowSince = time.Time{}
		for _, sl := range s.slots {
			sl.ecoSleep = false
		}
	}
}

// EcoMode reports whether Eco Mode is currently enabled.
func (s *Scheduler) EcoMode() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ecoMode
}

// isFamilyEligibleLocked mirrors AcquireFor's eligibility check for a single family.
func isFamilyEligibleLocked(sl *Slot, f Family, now time.Time) bool {
	if !sl.present || sl.ip(f) == nil {
		return false
	}
	if sl.state[f] == stateDown || now.Before(sl.downUntil[f]) {
		return false
	}
	return true
}

// updateEcoLocked updates ecoSleep flags for all slots. Caller must hold s.mu.
func (s *Scheduler) updateEcoLocked(now time.Time, anyGame bool, totalRate float64) {
	if !s.ecoMode || anyGame || totalRate >= ecoLowTrafficThreshold {
		s.ecoLowSince = time.Time{}
		for _, sl := range s.slots {
			sl.ecoSleep = false
		}
		return
	}

	if s.ecoLowSince.IsZero() {
		s.ecoLowSince = now
		return
	}
	if now.Sub(s.ecoLowSince) < ecoIdleHoldDuration {
		return
	}

	// Pick the best eligible primary adapter (preferring probed-up adapters,
	// then highest total weight across eligible address families).
	var primary *Slot
	bestProbedUp := false
	bestW := -1.0

	for _, sl := range s.slots {
		w := 0.0
		eligible := false
		probedUp := false
		for _, f := range []Family{Fam4, Fam6} {
			if isFamilyEligibleLocked(sl, f, now) {
				eligible = true
				w += sl.weight[f]
				if sl.state[f] == stateUp {
					probedUp = true
				}
			}
		}
		if !eligible {
			continue
		}
		if primary == nil || (probedUp && !bestProbedUp) || (probedUp == bestProbedUp && w > bestW) {
			primary = sl
			bestProbedUp = probedUp
			bestW = w
		}
	}

	for _, sl := range s.slots {
		sl.ecoSleep = (primary != nil && sl != primary)
	}
}
