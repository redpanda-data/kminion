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
kminion_kafka_consumer_group_info{coordinator_id="0",group_id="bigquery-sink",member_count="2",protocol="range",protocol_type="consumer",state="Stable"} 1

# HELP kminion_kafka_consumer_group_topic_offset_sum The sum of all committed group offsets across all partitions in a topic
# TYPE kminion_kafka_consumer_group_topic_offset_sum gauge
kminion_kafka_consumer_group_topic_offset_sum{group_id="bigquery-sink",topic_name="shop-activity"} 4.259513e+06

# HELP kminion_kafka_consumer_group_topic_partition_lag The number of messages a consumer group is lagging behind the latest offset of a partition
# TYPE kminion_kafka_consumer_group_topic_partition_lag gauge
kminion_kafka_consumer_group_topic_partition_lag{group_id="bigquery-sink",partition_id="10",topic_name="shop-activity"} 147481

# HELP kminion_kafka_consumer_group_topic_lag The number of messages a consumer group is lagging behind across all partitions in a topic
# TYPE kminion_kafka_consumer_group_topic_lag gauge
kminion_kafka_consumer_group_topic_lag{group_id="bigquery-sink",topic_name="shop-activity"} 147481
```

#### Offset Commits Metrics
The following metrics are only available when KMinion is configured to use `scrapeMode: offsetsTopic`.
```
# HELP kminion_kafka_consumer_group_offset_commits_total The number of offsets committed by a group
# TYPE kminion_kafka_consumer_group_offset_commits_total counter
kminion_kafka_consumer_group_offset_commits_total{group_id="bigquery-sink"} 1098
```

