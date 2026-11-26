package ml

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

type FeatureSample struct {
	RxPPS         float64
	TxPPS         float64
	SmallPktRatio float64
	AvgPktSize    float64
	IATVariance   float64
}

var (
	csvOnce    sync.Once
	csvMu      sync.Mutex
	csvWriter  *csv.Writer
	csvEnabled bool
	csvLabel   int
)

func initSharedCSV(csvPath string) {
	csvOnce.Do(func() {
		val := os.Getenv("NETMERGE_COLLECT_LABEL")
		if val == "" {
			return
		}
		label, err := strconv.Atoi(val)
		if err != nil {
			return
		}
		f, err := os.OpenFile(csvPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ML] 無法開啟錄製檔 %s: %v\n", csvPath, err)
			return
		}
		w := csv.NewWriter(f)
		if stat, _ := f.Stat(); stat.Size() == 0 {
			_ = w.Write([]string{"rx_pps", "tx_pps", "small_ratio", "avg_size", "iat_var", "label"})
			w.Flush()
		}
		csvWriter = w
		csvLabel = label
		csvEnabled = true
		fmt.Fprintf(os.Stderr, "[ML] 特徵錄製已啟動：label=%d -> %s\n", csvLabel, csvPath)
	})
}

type WindowCollector struct {
	windowStart time.Time
	lastPktTime time.Time
	rxCount     int
	txCount     int
	smallCount  int
	totalBytes  int
	iatCount    int
	sumIATMs    float64
	sumIATMsSq  float64
}

func NewWindowCollector(csvPath string) *WindowCollector {
	initSharedCSV(csvPath)
	return &WindowCollector{windowStart: time.Now()}
}

// RecordPacket 呼叫端需自行持有鎖（flowconn.go 的 u.mu 已保護）
func (c *WindowCollector) RecordPacket(pktBytes int, isRx bool, now time.Time) (FeatureSample, bool) {
	if isRx {
		c.rxCount++
	} else {
		c.txCount++
	}
	if pktBytes < 1200 {
		c.smallCount++
	}
	c.totalBytes += pktBytes

	if !c.lastPktTime.IsZero() {
		dtMs := float64(now.Sub(c.lastPktTime).Microseconds()) / 1000.0
		if dtMs >= 0 {
			c.sumIATMs += dtMs
			c.sumIATMsSq += dtMs * dtMs
			c.iatCount++
		}
	}
	c.lastPktTime = now

	elapsed := now.Sub(c.windowStart).Seconds()
	if elapsed < 2.0 {
		return FeatureSample{}, false
	}

	totalPkts := c.rxCount + c.txCount
	var smallRatio, avgSize, iatVar float64
	if totalPkts > 0 {
		smallRatio = float64(c.smallCount) / float64(totalPkts)
		avgSize = float64(c.totalBytes) / float64(totalPkts)
	}
	if c.iatCount > 1 {
		mean := c.sumIATMs / float64(c.iatCount)
		iatVar = (c.sumIATMsSq / float64(c.iatCount)) - (mean * mean)
		if iatVar < 0 {
			iatVar = 0
		}
	}

	sample := FeatureSample{
		RxPPS:         float64(c.rxCount) / elapsed,
		TxPPS:         float64(c.txCount) / elapsed,
		SmallPktRatio: smallRatio,
		AvgPktSize:    avgSize,
		IATVariance:   iatVar,
	}

	// 至少 4 個封包/2秒才寫入，避免零星背景心跳包污染資料集
	if csvEnabled && totalPkts >= 4 {
		csvMu.Lock()
		_ = csvWriter.Write([]string{
			fmt.Sprintf("%.4f", sample.RxPPS),
			fmt.Sprintf("%.4f", sample.TxPPS),
			fmt.Sprintf("%.4f", sample.SmallPktRatio),
			fmt.Sprintf("%.2f", sample.AvgPktSize),
			fmt.Sprintf("%.4f", sample.IATVariance),
			strconv.Itoa(csvLabel),
		})
		csvWriter.Flush()
		csvMu.Unlock()
	}

	c.windowStart = now
	c.rxCount, c.txCount, c.smallCount, c.totalBytes, c.iatCount = 0, 0, 0, 0, 0
	c.sumIATMs, c.sumIATMsSq = 0, 0
	return sample, true
}