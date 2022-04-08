package benefits

import (
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"github.com/gonum/stat"
)

type NormalizationFunc func(calculator interfaces2.Calculator, benefit float64) float64

type Normalizer interface {
	CloneStub(stub interface{}, cloneStore bool) interface{}
	UpdateStub(stub interface{}, benefit ...float64)
	Normalize(benefit float64, stub interface{}) float64
	NewStub() interface{}
}

type ZScoreNormalizer struct {
}

type ZScoreNormalizerStub struct {
	store  []float64
	Mean   float64
	StdDev float64
}

func NewZScoreNormalizer() *ZScoreNormalizer {
	n := &ZScoreNormalizer{}
	return n
}

func (z *ZScoreNormalizer) NewStub() interface{} {
	return &ZScoreNormalizerStub{
		Mean:   0,
		StdDev: 0,
	}
}

func (z *ZScoreNormalizer) UpdateStub(stub interface{}, benefit ...float64) {
	if stub == nil {
		stub = z.NewStub()
	}
	s := stub.(*ZScoreNormalizerStub)
	s.store = append(s.store, benefit...)
	mean, stdDev := stat.MeanStdDev(s.store, nil)
	s.StdDev = stdDev
	s.Mean = mean
}

func (z *ZScoreNormalizer) Normalize(benefit float64, stub interface{}) float64 {
	s := stub.(*ZScoreNormalizerStub)
	return (benefit - s.Mean) / s.StdDev
}

func (z *ZScoreNormalizer) CloneStub(stub interface{}, cloneStore bool) interface{} {
	s := stub.(*ZScoreNormalizerStub)
	ns := z.NewStub().(*ZScoreNormalizerStub)
	if cloneStore {
		ns.store = make([]float64, len(s.store))
		copy(ns.store, s.store)
	}
	ns.StdDev = s.StdDev
	ns.Mean = s.Mean
	return ns
}
