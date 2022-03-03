package events

type EventHandler interface {
	HandleEvent(*Event)
}

type Event struct {
	Data interface{}
	ResultChan chan *Result
}

type Result struct {
	Succeeded bool
	Reason string
}