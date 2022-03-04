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

func Reply(resultChan chan *Result, result *Result) {
	if resultChan != nil {
		resultChan <- result
	}
}

func ReplySucceeded(resultChan chan *Result) {
	Reply(resultChan, &Result{
		Succeeded: true,
	})
}