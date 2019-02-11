package main

// Topic contains aggregated partition details for a given Kafka topic
type Topic struct {
	Name                 string
	MessageCount         int64
	ReplicaCount         int32
	InSyncReplicaCount   int32
	PartitionDetailsByID map[int32]PartitionDetails
}
