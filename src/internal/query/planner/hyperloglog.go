package planner

import (
	"math"
	"math/bits"

	"github.com/cespare/xxhash/v2"
)

const (
	hllPrecision = 12                    // 12-bit precision
	hllRegisters = 1 << hllPrecision     // 4096 registers
	hllAlpha     = 0.7213 / (1.0 + 1.079/float64(hllRegisters))
)

// HyperLogLog implements the HyperLogLog++ algorithm for cardinality estimation.
// Memory: 4096 bytes (one byte per register at 12-bit precision).
// Standard error: ~1.6%.
type HyperLogLog struct {
	Registers [hllRegisters]uint8
}

// NewHyperLogLog creates a new HyperLogLog sketch.
func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{}
}

// Add inserts a value into the sketch.
func (hll *HyperLogLog) Add(value []byte) {
	hash := xxhash.Sum64(value)
	idx := hash >> (64 - hllPrecision)
	remaining := (hash << hllPrecision) | (1 << (hllPrecision - 1))
	runLen := uint8(bits.LeadingZeros64(remaining)) + 1

	if runLen > hll.Registers[idx] {
		hll.Registers[idx] = runLen
	}
}

// AddString is a convenience method for string values.
func (hll *HyperLogLog) AddString(s string) {
	hll.Add([]byte(s))
}

// Count returns the estimated cardinality.
func (hll *HyperLogLog) Count() float64 {
	sum := 0.0
	zeros := 0
	for _, val := range hll.Registers {
		sum += math.Pow(2.0, -float64(val))
		if val == 0 {
			zeros++
		}
	}

	m := float64(hllRegisters)
	estimate := hllAlpha * m * m / sum

	// Small range correction (linear counting)
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros))
	}

	return estimate
}

// Merge combines another HyperLogLog sketch into this one (union).
func (hll *HyperLogLog) Merge(other *HyperLogLog) {
	if other == nil {
		return
	}
	for i := range hll.Registers {
		if other.Registers[i] > hll.Registers[i] {
			hll.Registers[i] = other.Registers[i]
		}
	}
}
