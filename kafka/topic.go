package kafka

// Topic contains the topic name and all it's partitions' details
type Topic struct {
	Name         string
	MessageCount int64
	Partitions   map[int32]*Partition
}

type brokerOffsetResponse struct {
	partitionOffsets []topicPartitionOffset
	errorTopics      []string
}

type topicPartitionOffset struct {
	TopicName   string
	PartitionID int32
	Offset      int64
	OffsetType  offsetType
}

type offsetType int

const (
	newestOffset offsetType = 0
	oldestOffset offsetType = 1
)

// HighWaterMarkByPartitionID returns the highest commited offset for a given partitionID in this topic
func (t *Topic) HighWaterMarkByPartitionID(partitionID int32) int64 {
	return t.Partitions[partitionID].HighWaterMark
}

// Partition contains the PartitionID and it's highest commited offset (high water mark)
type Partition struct {
	PartitionID   int32
	HighWaterMark int64
	OldestOffset  int64
}

// MessageCount returns the difference between HighWaterMark and OldestOffset
func (p *Partition) MessageCount() int64 {
	return p.HighWaterMark - p.OldestOffset
}
