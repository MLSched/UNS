package cluster

import "UNS/schedulers/partition"

var instance *Manager = nil

func InitClusterManager() {
	instance = NewManager()
}

func GetManagerInstance() *Manager {
	return instance
}

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

func (m *Manager) AddClusterContext(ctx *Context) {
	m.resourceManagerID2ClusterContext[ctx.Meta.GetResourceManagerID()] = ctx
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
