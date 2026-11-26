package engine

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"net-merge/internal/ml"
	"net-merge/internal/nics"
)

// slotConn counts a pinned TCP flow's bytes and releases its slot on Close. It exposes
// CloseRead/CloseWrite because the TUN relay half-closes each direction separately.
type slotConn struct {
	*net.TCPConn
	sl       *Slot
	fam      Family
	s        *Scheduler
	once     sync.Once
	activity *flowActivity
}

func (c *slotConn) Read(p []byte) (int, error) {
	if pair := c.sl.limit.Load(); pair != nil && len(p) > pair.chunk {
		p = p[:pair.chunk] // hand out a chunk at a time, so a limited flow stays smooth
	}
	n, err := c.TCPConn.Read(p)
	if n > 0 {
		c.sl.bytesIn[c.fam].Add(int64(n))
		if c.activity != nil {
			c.activity.bytes.Add(int64(n))
		}
		c.sl.throttle(true, n)
	}
	return n, err
}

func (c *slotConn) Write(p []byte) (int, error) {
	n, err := c.sl.writePaced(c.TCPConn, p, false)
	c.sl.bytesOut[c.fam].Add(int64(n))
	if c.activity != nil {
		c.activity.bytes.Add(int64(n))
	}
	return n, err
}

// release frees the slot's balancing count. It happens at the first half-close, not at the final
// Close: the relay can keep a finished flow open for up to a minute waiting for the peer's FIN,
// and counting those as active would steer new flows away from the NIC that just finished.
func (c *slotConn) release() {
	c.once.Do(func() {
		c.s.untrack(c.sl, c.fam, c.activity)
		c.s.ReleaseFor(c.sl, c.fam)
	})
}

func (c *slotConn) CloseRead() error  { c.release(); return c.TCPConn.CloseRead() }
func (c *slotConn) CloseWrite() error { c.release(); return c.TCPConn.CloseWrite() }
func (c *slotConn) Close() error      { c.release(); return c.TCPConn.Close() }

// DialPinned is DialTCP for front-ends that hand the conn to someone else to pump.
func (b *Balancer) DialPinned(ctx context.Context, host string, port int) (net.Conn, error) {
	c, sl, f, err := b.DialTCP(ctx, host, port)
	if err != nil {
		return nil, err
	}
	return &slotConn{TCPConn: c.(*net.TCPConn), sl: sl, fam: f, s: b.S, activity: b.S.track(sl, f)}, nil
}

// udpFlowActivity tracks per-socket bidirectional UDP packet statistics to detect real-time interactive traffic.
type udpFlowActivity struct {
	mu                   sync.Mutex
	started, lastActive  time.Time
	winStart             time.Time
	winRxPkts, winTxPkts int64
	winBytes             int64
	rxPkts, txPkts       int64
	smallPkts, bytes     int64

	collector  *ml.WindowCollector
	lastSample ml.FeatureSample
	hasSample  bool
}

func (u *udpFlowActivity) record(n int, addr net.Addr, now time.Time, isOut bool) {
	// Skip system broadcast/discovery ports; keep UDP 443 for Steam/CS2 SDR relays.
	if ua, ok := addr.(*net.UDPAddr); ok {
		if ua.Port == 53 || ua.Port == 123 || ua.Port == 1900 || ua.Port == 5353 {
			return
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()

	// Allow up to 3.5s gap so Alt-Tab frame throttling does not reset active game state.
	if u.started.IsZero() || now.Sub(u.lastActive) > 3500*time.Millisecond {
		u.started = now
		u.winStart = now
		u.rxPkts, u.txPkts, u.smallPkts, u.bytes = 0, 0, 0, 0
		u.winRxPkts, u.winTxPkts, u.winBytes = 0, 0, 0
	}
	if now.After(u.lastActive) {
		u.lastActive = now
	}

	if isOut {
		u.txPkts++
	} else {
		u.rxPkts++
	}
	u.bytes += int64(n)
	if n < 1200 {
		u.smallPkts++
	}

	if now.Sub(u.winStart) >= 2*time.Second {
		u.winStart = now
		u.winRxPkts, u.winTxPkts, u.winBytes = 0, 0, 0
	}
	if isOut {
		u.winTxPkts++
	} else {
		u.winRxPkts++
	}
	u.winBytes += int64(n)
	
	if u.collector == nil {
		u.collector = ml.NewWindowCollector("traffic_features.csv")
	}
	if sample, ready := u.collector.RecordPacket(n, !isOut, now); ready {
		u.lastSample = sample
		u.hasSample = true
	}
		
}

func (u *udpFlowActivity) isGameLike(now time.Time) bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	// 保留 3.5 秒 Alt-Tab 降頻容忍與 5 秒持續門檻，作為時序平滑後處理
	if u.started.IsZero() || now.Sub(u.lastActive) > 3500*time.Millisecond {
		return false
	}
	if now.Sub(u.started).Seconds() < 5.0 || !u.hasSample {
		return false
	}

	// 改由權重折疊後的 Edge ML 模型推論互動流量機率
	return ml.PredictInteractiveProb(u.lastSample) >= 0.5
}

type slotPacketConn struct {
	net.PacketConn
	sl   *Slot
	fam  Family
	s    *Scheduler
	once sync.Once
	udp  *udpFlowActivity
}

func (c *slotPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, a, err := c.PacketConn.ReadFrom(p)
	if n > 0 {
		c.sl.bytesIn[c.fam].Add(int64(n))
		if c.udp != nil {
			c.udp.record(n, a, c.s.now(), false)
		}
		c.sl.throttlePacket(true, n)
	}
	return n, a, err
}

func (c *slotPacketConn) WriteTo(p []byte, a net.Addr) (int, error) {
	c.sl.throttlePacket(false, len(p))
	n, err := c.PacketConn.WriteTo(p, a)
	if n > 0 {
		c.sl.bytesOut[c.fam].Add(int64(n))
		if c.udp != nil {
			c.udp.record(n, a, c.s.now(), true)
		}
	}
	return n, err
}

// ListenUDP opens a UDP socket pinned to one NIC (a UDP "flow" = the socket, so every datagram
// of that flow leaves the same way). UDP flows are short and numerous (DNS), so they don't hold
// a balancing slot: the pick still spreads them via the scheduler's tie-breaking.
func (b *Balancer) ListenUDP(ctx context.Context, f Family) (net.PacketConn, error) {
	network := "udp4"
	if f == Fam6 {
		network = "udp6"
	}
	tried := map[string]struct{}{}
	lastErr := errors.New("no usable network adapter")
	for {
		sl := b.S.AcquireFor(f, tried)
		if sl == nil {
			return nil, lastErr
		}
		tried[sl.key] = struct{}{}
		lc := net.ListenConfig{Control: nics.ForceEgress(sl.Index, f == Fam6)}
		pc, err := lc.ListenPacket(ctx, network, net.JoinHostPort(sl.ip(f).String(), "0"))
		b.S.ReleaseFor(sl, f)
		if err != nil {
			if classify(err).linkDown {
				b.S.Fail(sl, f, b.FailCooldown)
			}
			lastErr = err
			continue
		}
		return &slotPacketConn{PacketConn: pc, sl: sl, fam: f, s: b.S, udp: b.S.trackUDP(sl, f)}, nil
	}
}

func (c *slotPacketConn) Close() error {
	c.once.Do(func() {
		if c.s != nil && c.udp != nil {
			c.s.untrackUDP(c.sl, c.fam, c.udp)
		}
	})
	return c.PacketConn.Close()
}
