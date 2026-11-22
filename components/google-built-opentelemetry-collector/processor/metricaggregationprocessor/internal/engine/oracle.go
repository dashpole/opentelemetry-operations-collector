// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"math"
	"math/big"
)

const oraclePrec = 256

// OracleStats contains arbitrary-precision (256-bit big.Float) reference values.
type OracleStats struct {
	Count     uint64
	Sum       float64
	Mean      float64
	M2        float64
	StdDev    float64
	PopStdDev float64
	Min       float64
	Max       float64
}

// ComputeExactOracle computes exact sliding-window statistics over values using
// 256-bit big.Float arithmetic (completely immune to float64 catastrophic
// cancellation and subtraction drift).
func ComputeExactOracle(values []float64) OracleStats {
	n := len(values)
	if n == 0 {
		return OracleStats{}
	}
	sum := new(big.Float).SetPrec(oraclePrec)
	minV := values[0]
	maxV := values[0]
	for _, x := range values {
		if x < minV {
			minV = x
		}
		if x > maxV {
			maxV = x
		}
		xf := new(big.Float).SetPrec(oraclePrec).SetFloat64(x)
		sum.Add(sum, xf)
	}
	nf := new(big.Float).SetPrec(oraclePrec).SetUint64(uint64(n))
	mean := new(big.Float).SetPrec(oraclePrec).Quo(sum, nf)

	m2 := new(big.Float).SetPrec(oraclePrec)
	diff := new(big.Float).SetPrec(oraclePrec)
	sq := new(big.Float).SetPrec(oraclePrec)
	for _, x := range values {
		xf := new(big.Float).SetPrec(oraclePrec).SetFloat64(x)
		diff.Sub(xf, mean)
		sq.Mul(diff, diff)
		m2.Add(m2, sq)
	}

	sumF, _ := sum.Float64()
	meanF, _ := mean.Float64()
	m2F, _ := m2.Float64()

	var sdSample, sdPop float64
	if n > 1 {
		denom := new(big.Float).SetPrec(oraclePrec).SetUint64(uint64(n - 1))
		varSample := new(big.Float).SetPrec(oraclePrec).Quo(m2, denom)
		vf, _ := varSample.Float64()
		if vf > 0 {
			sdSample = math.Sqrt(vf)
		}
	}
	varPop := new(big.Float).SetPrec(oraclePrec).Quo(m2, nf)
	vpf, _ := varPop.Float64()
	if vpf > 0 {
		sdPop = math.Sqrt(vpf)
	}

	return OracleStats{
		Count:     uint64(n),
		Sum:       sumF,
		Mean:      meanF,
		M2:        m2F,
		StdDev:    sdSample,
		PopStdDev: sdPop,
		Min:       minV,
		Max:       maxV,
	}
}
