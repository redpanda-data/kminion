# Kafka Minion

![License](https://img.shields.io/github/license/cloudworkz/kafka-minion.svg?color=blue) [![Go Report Card](https://goreportcard.com/badge/github.com/cloudworkz/kafka-minion)](https://goreportcard.com/report/github.com/cloudworkz/kafka-minion) ![GitHub release](https://img.shields.io/github/release/cloudworkz/kafka-minion.svg) [![Docker Repository on Quay](https://quay.io/repository/google-cloud-tools/kafka-minion/status "Docker Repository on Quay")](https://quay.io/repository/google-cloud-tools/kafka-minion)

Kafka minion is a prometheus exporter for Apache Kafka (v0.11.0.2+), created to reliably expose consumer group lag information along with other helpful, unique metrics. Easy to setup on Kubernetes environments.

![Grafana Dashboard for Kafka Consumer Group Lag Monitoring](https://raw.githubusercontent.com/cloudworkz/kafka-minion/master/docs/screenshots/grafana-main-1.png)

![Grafana Dashboard for Kafka Consumer Group Lag Monitoring](https://raw.githubusercontent.com/cloudworkz/kafka-minion/master/docs/screenshots/grafana-main-2.png)

![Grafana Dashboard for Kafka Topic Monitoring](https://raw.githubusercontent.com/cloudworkz/kafka-minion/master/docs/screenshots/grafana-ops-1.png)

![Grafana Dashboard for Kafka Cluster Monitoring](https://raw.githubusercontent.com/cloudworkz/kafka-minion/master/docs/screenshots/grafana-ops-2.png)


## Features

- [x] Supports Kafka 0.11.0.2 - 2.2.x (last updated 16th Jun 2019)
- [x] Fetches consumer group information directly from `__consumer_offsets` topic instead of querying single brokers for consumer group lags to ensure robustness in case of broker failures or leader elections
- [x] Kafka SASL/SSL support
- [x] Provides per consumergroup:topic lag metrics (removes a topic's metrics if a single partition metric in that topic couldn't be fetched)
- [x] Created to use in Kubernetes clusters (has liveness/readiness check and helm chart for easier setup)
- [x] No Zookeeper dependencies

## Roadmap

- [ ] Adding more tests, especially for decoding all the kafka binary messages. The binary format sometimes changes with newer kafka versions. To ensure that all kafka versions will be supported and future kafka minion changes are compatible, I'd like to add tests on this
- [ ] Getting more feedback from users who run Kafka Minion in other environments
- [x] **DONE:** Add sample Grafana dashboard
- [x] **DONE:** Add more metrics about topics and partitions (partition count and cleanup policy)

## Setup

### Helm Chart

Kubernetes users may want to use the Helm chart to deploy Kafka Minion: https://github.com/cloudworkz/kafka-minion-helm-chart

### Environment variables

| Variable name | Description | Default |
| --- | --- | --- |
| TELEMETRY_HOST | Host to listen on for the prometheus exporter | 0.0.0.0 |
| TELEMETRY_PORT | HTTP Port to listen on for the prometheus exporter | 8080 |
| LOG_LEVEL | Log granularity (debug, info, warn, error, fatal, panic) | info |
| VERSION | Application version (env variable is set in Dockerfile) | (from Dockerfile) |
| EXPORTER_IGNORE_SYSTEM_TOPICS | Don't expose metrics about system topics (any topic names which are "\_\_" or "\_confluent" prefixed) | true |
| METRICS_PREFIX | A prefix for all exported prometheus metrics | kafka_minion |
| KAFKA_BROKERS | Array of broker addresses, delimited by comma (e. g. "kafka-1:9092, kafka-2:9092") | (No default) |
| KAFKA_CONSUMER_OFFSETS_TOPIC_NAME | Topic name of topic where kafka commits the consumer offsets | \_\_consumer_offsets |
| KAFKA_SASL_ENABLED | Bool to enable/disable SASL authentication (only SASL_PLAINTEXT is supported) | false |
| KAFKA_SASL_USE_HANDSHAKE | Whether or not to send the Kafka SASL handshake first | true |
| KAFKA_SASL_USERNAME | SASL Username | (No default) |
| KAFKA_SASL_PASSWORD | SASL Password | (No default) |
| KAFKA_TLS_ENABLED | Whether or not to use TLS when connecting to the broker | false |
| KAFKA_TLS_CA_FILE_PATH | Path to the TLS CA file | (No default) |
| KAFKA_TLS_KEY_FILE_PATH | Path to the TLS key file | (No default) |
| KAFKA_TLS_CERT_FILE_PATH | Path to the TLS cert file | (No default) |
| KAFKA_TLS_INSECURE_SKIP_TLS_VERIFY | If true, TLS accepts any certificate presented by the server and any host name in that certificate. | true |
| KAFKA_TLS_PASSPHRASE | Passphrase to decrypt the TLS Key | (No default) |

### Grafana Dashboard

You can import our suggested Grafana dashboards and modify them as you wish:
 - https://grafana.com/dashboards/10083 (Kafka Minion Dashboard ID 10083)
 - https://grafana.com/dashboards/10466 (Kafka Minion OPS Dashbaord ID 10466)

## Exposed metrics

### Labels

Below metrics have a variety of different labels, explained in this section:

**`topic`**: Topic name

**`partition`**: Partition ID (partitions are zero indexed)

**`group`**: The consumer group name

**`group_version`**: Instead of resetting offsets some developers replay data by creating a new group and increment an appending number ("sample-group" -> "sample-group-1"). Group names without an appending number are considered as version 0.

**`group_base_name`**: The base name is the part of the group name which prefixes the group_version. For "sample-group-1" it is "sample-group-".

**`group_is_latest`** Assuming you have multiple consumer groups with the same base name, but different versions this label indicates if this group is the one with the highest version amongst all other known consumer groups. If there is "sample-group", "sample-group-1" and "sample-group-2" only the least mentioned group has `group_is_lastest` set to "true".

### Metrics

#### Consumer group metrics

| Metric | Description |
| --- | --- |
| `kafka_minion_group_topic_lag{group, group_base_name, group_is_latest, group_version, topic}` | Number of messages the consumer group is behind for a given topic. |
| `kafka_minion_group_topic_partition_lag{group, group_base_name, group_is_latest, group_version, topic, partition}` | Number of messages the consumer group is behind for a given partition. |
| `kafka_minion_group_topic_partition_offset{group, group_base_name, group_is_latest, group_version, topic, partition}` | Current offset of a given group on a given partition. |
| `kafka_minion_group_topic_partition_commit_count{group, group_base_name, group_is_latest, group_version, topic, partition}` | Number of commited offset entries by a consumer group for a given partition. Helpful to determine the commit rate to possibly tune the consumer performance. |
| `kafka_minion_group_topic_partition_last_commit{group, group_base_name, group_is_latest, group_version, topic, partition}` | Timestamp of last consumer group commit on a given partition |

#### Topic / Partition metrics

| Metric | Description |
| --- | --- |
| `kafka_minion_topic_partition_count{topic, cleanup_policy}` | Partition count for a given topic along with cleanup policy as label |
| `kafka_minion_topic_partition_high_water_mark{topic, partition}` | Latest known commited offset for this partition. This metric is being updated periodically and thus the actual high water mark may be ahead of this one. |
| `kafka_minion_topic_partition_low_water_mark{topic, partition}` | Oldest known commited offset for this partition. This metric is being updated periodically and thus the actual high water mark may be ahead of this one. |
| `kafka_minion_topic_partition_message_count{topic, partition}` | Number of messages for a given partition. Calculated by subtracting high water mark by low water mark. Thus this metric is likely to be invalid for compacting topics, but it still can be helpful to get an idea about the number of messages in that topic. |
| `kafka_minion_topic_subscribed_groups_count{topic}` | Number of consumer groups which have at least one consumer group offset for any of the topic's partitions |

#### Internal metrics

| Metric | Description |
| --- | --- |
| `kafka_minion_internal_offset_consumer_offset_commits_read{version}` | Number of read offset commit messages |
| `kafka_minion_internal_offset_consumer_offset_commits_tombstones_read{version}` | Number of tombstone messages of all offset commit messages |
| `kafka_minion_internal_offset_consumer_group_metadata_read{version}` | Number of read group metadata messages |
| `kafka_minion_internal_offset_consumer_group_metadata_tombstones_read{version}` | Number of tombstone messages of all group metadata messages |
| `kafka_minion_internal_kafka_messages_in_success{topic}` | Number of successfully received kafka messages |
| `kafka_minion_internal_kafka_messages_in_failed{topic}` | Number of errors while consuming kafka messages |

## How does it work

At a high level Kafka Minion fetches data from two different sources (see below). Kafka Minion provides lots of metrics by connecting these datasets. For instance a partition high water mark with a consumer group's current offset to calculate the lag on that partition. Invocating the `/metrics` endpoint starts the calculation of these metrics on a snapshot of the current data.

- **Consumer Group Data:** Since Kafka version 0.10 Zookeeper is no longer in charge of maintaining the consumer group offsets. Instead Kafka itself utilizes an internal Kafka topic called `__consumer_offsets`. Messages in that topic are binary and the protocol may change with broker upgrades. On each succesful offset commit from a consumer group member a message is created and produced to that topic. The message key is a combination of the `groupId`, `topic` and `partition`. The value is the offset index.

  The `__consumer_offsets` topic is a compacted topic. Once an offset expires Kafka produces a tombstone for the given key, which will Kafka Minion use to delete the offset information as well. Therefore Kafka Minion has to consume all messages from earliest, so that it gets all consumer group offsets which have not yet been expired.

- **Broker requests:** Brokers are being queried to get topic metadata information, such as partition count, topic configuration, low & high water mark.

## FAQ

### Why did you create yet another kafka lag exporter?

1. As of writing the exporter there is no publicly available prometheus exporter (to my knowledge) which is lightweight, robust and supports Kafka v0.11 - v2.1+

2. We are primarily interested in per consumergroup:topic lags. Some exporters export either only group lags of all topics altogether or they export only per partition metrics. While you can obviously aggregate those partition metrics in Grafana as well, this adds unnecessary complexity in Grafana dashboards. This exporter offers metrics on partition and topic granularity.

3. In our environment developers occasionally reconsume a topic by creating a new consumer group. They do so by incrementing a trailing number (e. g. "sample-group-1" becomes "sample-group-2"). In order to setup a proper alerting based on increasing lags for all consumer groups in a cluster, we need to ignore those "outdated" consumer groups. In this illustration "sample-group-1" as it's not being used anymore. This exporter adds 3 labels on each exporter consumergroup:topic lag metric to make that possible: `group_base_name`, `group_version`, `group_is_latest`. The meaning of each label is explained in the section [Labels](#labels)

4. More (unique) metrics which are not available in other exporters. Likely some of these metrics and features are not desired in other exporters too.

### How does Kafka Minion compare to Burrow?

- Similiar data sources (consuming \_\_consumer_offsets topic and polling broker requests for topic watermarks)
- Kafka Minion offers prometheus metrics natively, while Burrow needs an additional metrics exporter
- Burrow has a more sophisticated approach to evaluate consumer lag health, while Kafka Minion leaves this to Grafana (query + alerts)
- Burrow supports multiple Kafka clusters, Kafka Minion is designed to be deployed once for each kafka cluster
- Kafka Minion is more lightweight and less complex because it does not offer such lag evaluation or multiple cluster support
- Kafka Minion offers different/additional metrics and labels which aren't offered by Burrow and vice versa
- Kafka Minion does not support consumer groups which still commit to Zookeeper, therefore it doesn't has any Zookeeper dependencies while Burrow supports those
