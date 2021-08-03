# Exported Metrics

This document lists all exported metrics in an exemplary way.

## Exporter Metrics

```
# HELP kminion_exporter_up Build info about this Prometheus Exporter. Gauge value is 0 if one or more scrapes have failed.
# TYPE kminion_exporter_up gauge
kminion_exporter_up{version="sha-0ab0dcdf862f7a34b06998cd2d980148e048151a"} 1

# HELP kminion_exporter_offset_consumer_records_consumed_total The number of offset records that have been consumed by the internal offset consumer
# TYPE kminion_exporter_offset_consumer_records_consumed_total counter
kminion_exporter_offset_consumer_records_consumed_total 5.058244883e+09
```

## Kafka Metrics

### General / Cluster Metrics

```
# HELP kminion_kafka_broker_info Kafka broker information
# TYPE kminion_kafka_broker_info gauge
kminion_kafka_broker_info{address="broker-9.analytics-prod.kafka.cloudhut.dev",broker_id="9",is_controller="false",port="9092",rack_id="europe-west1-b"} 1

# HELP kminion_kafka_cluster_info Kafka cluster information
# TYPE kminion_kafka_cluster_info gauge
kminion_kafka_cluster_info{broker_count="12",cluster_id="UYZJg8bhT_6SxhsdaQZEQ",cluster_version="v2.6",controller_id="6"} 1
```

### Log Dir Metrics

```
# HELP kminion_kafka_broker_log_dir_size_total_bytes The summed size in bytes of all log dirs for a given broker
# TYPE kminion_kafka_broker_log_dir_size_total_bytes gauge
kminion_kafka_broker_log_dir_size_total_bytes{address="broker-9.analytics-prod.kafka.cloudhut.dev",broker_id="9",port="9092",rack_id="europe-west1-b"} 8.32654935115e+11

# HELP kminion_kafka_topic_log_dir_size_total_bytes The summed size in bytes of partitions for a given topic. This includes the used space for replica partitions.
# TYPE kminion_kafka_topic_log_dir_size_total_bytes gauge
kminion_kafka_topic_log_dir_size_total_bytes{topic_name="__consumer_offsets"} 9.026554258e+09
```

### Topic & Partition Metrics

```
# HELP kminion_kafka_topic_info Info labels for a given topic
# TYPE kminion_kafka_topic_info gauge
kminion_kafka_topic_info{cleanup_policy="compact",partition_count="1",replication_factor="1",topic_name="_confluent-ksql-default__command_topic"} 1

# HELP kminion_kafka_topic_partition_low_water_mark Partition Low Water Mark
# TYPE kminion_kafka_topic_partition_low_water_mark gauge
kminion_kafka_topic_partition_low_water_mark{partition_id="0",topic_name="__consumer_offsets"} 0

# HELP kminion_kafka_topic_low_water_mark_sum Sum of all the topic's partition low water marks
# TYPE kminion_kafka_topic_low_water_mark_sum gauge
kminion_kafka_topic_low_water_mark_sum{topic_name="__consumer_offsets"} 0

# HELP kminion_kafka_topic_partition_high_water_mark Partition High Water Mark
# TYPE kminion_kafka_topic_partition_high_water_mark gauge
kminion_kafka_topic_partition_high_water_mark{partition_id="0",topic_name="__consumer_offsets"} 2.04952001e+08

# HELP kminion_kafka_topic_high_water_mark_sum Sum of all the topic's partition high water marks
# TYPE kminion_kafka_topic_high_water_mark_sum gauge
kminion_kafka_topic_high_water_mark_sum{topic_name="__consumer_offsets"} 1.512023846873e+12
```

### Consumer Group Metrics

```
# HELP kminion_kafka_consumer_group_info Consumer Group info metrics. It will report 1 if the group is in the stable state, otherwise 0.
# TYPE kminion_kafka_consumer_group_info gauge
kminion_kafka_consumer_group_info{coordinator_id="0",group_id="bigquery-sink",protocol="range",protocol_type="consumer",state="Stable"} 1

# HELP kminion_kafka_consumer_group_members Consumer Group member count metrics. It will report the number of members in the consumer group
# TYPE kminion_kafka_consumer_group_members gauge
kminion_kafka_consumer_group_members{group_id="bigquery-sink"} 2

# HELP kminion_kafka_consumer_group_empty_members Consumer Group Empty Members. It will report the number of members in the consumer group with no partition assigned
# TYPE kminion_kafka_consumer_group_empty_members gauge
kminion_kafka_consumer_group_empty_members{group_id="bigquery-sink"} 1

# HELP kminion_kafka_consumer_group_topic_members Consumer Group topic member count metrics. It will report the number of members in the consumer group assigned on a given topic
# TYPE kminion_kafka_consumer_group_topic_members gauge
kminion_kafka_consumer_group_topic_members{group_id="bigquery-sink",topic_name="shop-activity"} 4

# HELP kminion_kafka_consumer_group_topic_assigned_partitions Consumer Group topic partitions count metrics. It will report the number of partitions assigned in the consumer group for a given topic
# TYPE kminion_kafka_consumer_group_topic_assigned_partitions gauge
kminion_kafka_consumer_group_topic_assigned_partitions{group_id="bigquery-sink",topic_name="shop-activity"} 32

# HELP kminion_kafka_consumer_group_topic_offset_sum The sum of all committed group offsets across all partitions in a topic
# TYPE kminion_kafka_consumer_group_topic_offset_sum gauge
kminion_kafka_consumer_group_topic_offset_sum{group_id="bigquery-sink",topic_name="shop-activity"} 4.259513e+06

# HELP kminion_kafka_consumer_group_topic_partition_lag The number of messages a consumer group is lagging behind the latest offset of a partition
# TYPE kminion_kafka_consumer_group_topic_partition_lag gauge
kminion_kafka_consumer_group_topic_partition_lag{group_id="bigquery-sink",partition_id="10",topic_name="shop-activity"} 147481

# HELP kminion_kafka_consumer_group_topic_lag The number of messages a consumer group is lagging behind across all partitions in a topic
# TYPE kminion_kafka_consumer_group_topic_lag gauge
kminion_kafka_consumer_group_topic_lag{group_id="bigquery-sink",topic_name="shop-activity"} 147481

# HELP kminion_kafka_consumer_group_offset_commits_total The number of offsets committed by a group
# TYPE kminion_kafka_consumer_group_offset_commits_total counter
kminion_kafka_consumer_group_offset_commits_total{group_id="bigquery-sink"} 1098
```

### End-to-End Metrics

```
# HELP kminion_end_to_end_messages_produced_total Number of messages that kminion's end-to-end test has tried to send to kafka
# TYPE kminion_end_to_end_messages_produced_total counter
kminion_end_to_end_messages_produced_total 384

# HELP kminion_end_to_end_commits_total Counts how many times kminions end-to-end test has committed messages
# TYPE kminion_end_to_end_commits_total counter
kminion_end_to_end_commits_total 18

# HELP kminion_end_to_end_messages_acked_total Number of messages kafka acknowledged as produced
# TYPE kminion_end_to_end_messages_acked_total counter
kminion_end_to_end_messages_acked_total 383

# HELP kminion_end_to_end_messages_received_total Number of *matching* messages kminion received. Every roundtrip message has a minionID (randomly generated on startup) and a timestamp. Kminion only considers a message a match if it it arrives within the configured roundtrip SLA (and it matches the minionID)
# TYPE kminion_end_to_end_messages_received_total counter
kminion_end_to_end_messages_received_total 383

# HELP kminion_end_to_end_produce_latency_seconds Time until we received an ack for a produced message
# TYPE kminion_end_to_end_produce_latency_seconds histogram
kminion_end_to_end_produce_latency_seconds_bucket{partitionId="0",le="0.005"} 0

# HELP kminion_end_to_end_commit_latency_seconds Time kafka took to respond to kminion's offset commit
# TYPE kminion_end_to_end_commit_latency_seconds histogram
kminion_end_to_end_commit_latency_seconds_bucket{groupCoordinatorBrokerId="0",le="0.005"} 0

# HELP kminion_end_to_end_roundtrip_latency_seconds Time it took between sending (producing) and receiving (consuming) a message
# TYPE kminion_end_to_end_roundtrip_latency_seconds histogram
kminion_end_to_end_roundtrip_latency_seconds_bucket{partitionId="0",le="0.005"} 0
```
