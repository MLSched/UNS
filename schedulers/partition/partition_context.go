package partition

import "UNS/pb_gen/objects"

type Context struct {
	*objects.Partition
}

func Build(partition *objects.Partition) (*Context, error) {
	return &Context{
		partition,
	}, nil
}