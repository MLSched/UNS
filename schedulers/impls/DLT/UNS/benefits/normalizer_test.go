package benefits

import (
	"testing"
)

func TestNewZScoreNormalizer(t *testing.T) {
	n := NewZScoreNormalizer()
	stub := n.NewStub()
	n.UpdateStub(stub, 1, 2, 5)
	t.Log(n.Normalize(2.7, stub))
}
