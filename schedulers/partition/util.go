package partition

import (
	"UNS/pb_gen/objects"
	"encoding/json"
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
