package partition

import (
	"encoding/json"
	"github.com/MLSched/UNS/pb_gen/objects"
)

type Util struct {
}

func (u *Util) ExtractDLTJobExtra(job *objects.Job) (*objects.DLTJobExtra, error) {
	extra := &objects.DLTJobExtra{}
	err := json.Unmarshal(job.GetExtra(), &extra)
	if err != nil {
		return nil, err
	}
	return extra, nil
}
