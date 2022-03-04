package cluster

import (
	"UNS/pb_gen/objects"
	"UNS/schedulers/partition"
	"fmt"
)

type Context struct {
	Meta              *objects.Cluster
	partitionContexts map[string]*partition.Context
}

func Build(cluster *objects.Cluster) (*Context, error) {
	c := &Context{
		Meta:              cluster,
		partitionContexts: make(map[string]*partition.Context, len(cluster.GetPartitions())),
	}
	for _, partitionObj := range cluster.GetPartitions() {
		if _, ok := c.partitionContexts[partitionObj.GetPartitionID()]; ok {
			return nil, fmt.Errorf("cluster context build failed, duplicated partition ID [%s] found", partitionObj.GetPartitionID())
		}
		partitionContext, err := partition.Build(partitionObj)
		if err != nil {
			return nil, fmt.Errorf("cluster context build failed, due to the build of partition failed, failed partition ID = [%s], err = [%v]", partitionObj.GetPartitionID(), err)
		}
		c.partitionContexts[partitionObj.GetPartitionID()] = partitionContext
	}
	return c, nil
}

func (c *Context) GetPartitionContexts() map[string]*partition.Context {
	return c.partitionContexts
}

func (c *Context) GetPartitionContext(partitionID string) *partition.Context {
	return c.partitionContexts[partitionID]
}
