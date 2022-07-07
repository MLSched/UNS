package types

type GPUType string
type GPUID int

type GPU interface {
	ID() GPUID
	AccID() string
	Type() GPUType
	String() string
}
