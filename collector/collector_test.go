package collector

import (
	"github.com/google-cloud-tools/kafka-minion/kafka"
	"testing"
)

func TestGetVersionedConsumerGroups(t *testing.T) {
	offsets := make(map[string]*kafka.ConsumerPartitionOffset)
	offsets["sample-group-1"] = &kafka.ConsumerPartitionOffset{
		Group:     "sample-group-1",
		Topic:     "important-topic",
		Partition: 0,
		Offset:    1156,
		Timestamp: 1552723003465,
	}
	offsets["sample-group-2"] = &kafka.ConsumerPartitionOffset{
		Group:     "sample-group-2",
		Topic:     "important-topic",
		Partition: 0,
		Offset:    1000,
		Timestamp: 1552723003465,
	}
	offsets["sample-group-3"] = &kafka.ConsumerPartitionOffset{
		Group:     "sample-group-3",
		Topic:     "important-topic",
		Partition: 0,
		Offset:    1200,
		Timestamp: 1552723003475,
	}
	offsets["another-group"] = &kafka.ConsumerPartitionOffset{
		Group:     "another-group",
		Topic:     "important-topic",
		Partition: 0,
		Offset:    1200,
		Timestamp: 1552723003485,
	}
	offsets["another-group"] = &kafka.ConsumerPartitionOffset{
		Group:     "another-group",
		Topic:     "important-topic",
		Partition: 0,
		Offset:    936,
		Timestamp: 1552723003485,
	}

	tables := []struct {
		groupName    string
		groupVersion uint32
		baseName     string
		isLatest     bool
	}{
		{"sample-group-1", 1, "sample-group", false},
		{"sample-group-2", 2, "sample-group", false},
		{"sample-group-3", 3, "sample-group", true},
		{"another-group", 0, "another-group", true},
		{"console-consumer-40098", 40098, "console-consumer", true},
	}

	versionedGroups := getVersionedConsumerGroups(offsets)
	for _, table := range tables {
		baseName := versionedGroups[table.groupName].BaseName
		version := versionedGroups[table.groupName].Version
		isLatest := versionedGroups[table.groupName].IsLatest

		if baseName != table.baseName {
			t.Errorf("Expected base name for group %v was different. Expected: %v , Got: %v", table.groupName, table.baseName, baseName)
		}
		if version != table.groupVersion {
			t.Errorf("Expected version for group %v was different. Expected: %v , Got: %v", table.groupName, table.groupVersion, version)
		}
		if isLatest != table.isLatest {
			t.Errorf("Expected isLatest for group %v was different. Expected: %v , Got: %v", table.groupName, table.isLatest, isLatest)
		}
	}
}

func TestParseConsumerGroupName(t *testing.T) {
	tables := []struct {
		groupName string
		version   uint8
		baseName  string
		isLatest  bool
	}{
		{"sample-group-2", 2, "sample-group", false},
		{"sample-group-3", 3, "sample-group", false},
		{"another-group", 0, "another-group", false},
	}
	for _, table := range tables {
		versioned := parseConsumerGroupName(table.groupName)
		if table.groupName != versioned.Name {
			t.Errorf("Group name of %v was incorrect, got: %v, want: %v", table.groupName, versioned.Name, table.groupName)
		}
		if table.version != versioned.Version {
			t.Errorf("Version of %v was incorrect, got: %v, want: %v", table.groupName, versioned.Version, table.version)
		}
		if table.baseName != versioned.BaseName {
			t.Errorf("Base name of %v was incorrect, got: %v, want: %v", table.groupName, versioned.BaseName, table.baseName)
		}
		if table.isLatest != versioned.IsLatest {
			t.Errorf("IsLatest of %v was incorrect, got: %v, want: %v", table.groupName, versioned.IsLatest, table.isLatest)
		}
	}
}
