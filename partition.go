package main

// PartitionDetails contains offset and replica information for a kafka partition
type PartitionDetails struct {
	PartitionID        int32
	HighWaterMark      int64
	OldestOffset       int64
	MessageCount       int64
	ReplicaCount       uint8
	InSyncReplicaCount uint8
}
