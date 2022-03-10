package cluster

import (
	"UNS/schedulers/partition"
	"fmt"
)

type Manager struct {
	resourceManagerID2ClusterContext map[string]*Context
}

func NewManager() *Manager {
	return &Manager{
		resourceManagerID2ClusterContext: make(map[string]*Context),
	}
}

func (m *Manager) GetClusterContext(resourceManagerID string) *Context {
	return m.resourceManagerID2ClusterContext[resourceManagerID]
}

func (m *Manager) AddClusterContext(ctx *Context) error {
	if ctx, ok := m.resourceManagerID2ClusterContext[ctx.Meta.GetResourceManagerID()]; ok {
		return fmt.Errorf("cluster already exists, resource manager ID = %s", ctx.Meta.GetResourceManagerID())
	}
	m.resourceManagerID2ClusterContext[ctx.Meta.GetResourceManagerID()] = ctx
	return nil
}

func (m *Manager) RemoveClusterContext(resourceManagerID string) {
	delete(m.resourceManagerID2ClusterContext, resourceManagerID)
}

func (m *Manager) GetPartitionContext(resourceManagerID string, partitionID string) *partition.Context {
	clusterContext := m.GetClusterContext(resourceManagerID)
	if clusterContext == nil {
		return nil
	}
	return clusterContext.GetPartitionContext(partitionID)
}
