package metadata

import (
	"encoding/json"
	"time"
)

// Rebalance handles node failure by updating the cluster configuration.
// It returns a new ClusterConfig with the updated topology.
func Rebalance(config *ClusterConfig, deadNodeID string) *ClusterConfig {
	// Deep copy the config to avoid mutating the original
	newConfig := deepCopyConfig(config)

	// Increment Epoch
	newConfig.Epoch++

	// Mark node as Dead
	if status, exists := newConfig.Nodes[deadNodeID]; exists {
		status.Status = StatusDead
		status.LastHeartbeat = time.Time{} // Clear heartbeat
		newConfig.Nodes[deadNodeID] = status
	}

	// Reassign Shards
	for i, shard := range newConfig.Shards {
		if shard.PrimaryID == deadNodeID {
			if len(shard.ReplicaIDs) > 0 {
				// Promote first replica
				newPrimary := shard.ReplicaIDs[0]
				newConfig.Shards[i] = ShardMetadata{
					ID:         shard.ID,
					PrimaryID:  newPrimary,
					ReplicaIDs: shard.ReplicaIDs[1:],
				}
			} else {
				// No replicas available - mark as unavailable
				newConfig.Shards[i] = ShardMetadata{
					ID:         shard.ID,
					PrimaryID:  "",
					ReplicaIDs: shard.ReplicaIDs,
				}
			}
		} else {
			// If dead node was a replica, remove it
			newReplicas := make([]string, 0, len(shard.ReplicaIDs))
			for _, replica := range shard.ReplicaIDs {
				if replica != deadNodeID {
					newReplicas = append(newReplicas, replica)
				}
			}
			newConfig.Shards[i] = ShardMetadata{
				ID:         shard.ID,
				PrimaryID:  shard.PrimaryID,
				ReplicaIDs: newReplicas,
			}
		}
	}

	return newConfig
}

// deepCopyConfig creates a deep copy of ClusterConfig
func deepCopyConfig(config *ClusterConfig) *ClusterConfig {
	data, _ := json.Marshal(config)
	var newConfig ClusterConfig
	json.Unmarshal(data, &newConfig)
	return &newConfig
}
