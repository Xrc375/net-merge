package ml

import "testing"

func BenchmarkPredictInteractiveProb(b *testing.B) {
	sample := FeatureSample{
		RxPPS:         42.5,
		TxPPS:         18.2,
		SmallPktRatio: 0.92,
		AvgPktSize:    240.0,
		IATVariance:   12.4,
	}
	var sink float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = PredictInteractiveProb(sample)
	}
	_ = sink
}
