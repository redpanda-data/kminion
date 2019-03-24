# Kafka Minion (Alpha - Still in development)

Kafka minion is a prometheus exporter for Apache Kafka (v0.10.0+), created to expose consumer group lags on a per topic (rather than per partition) basis.

## Features

- [x] Supports Kafka 0.10.1.0 - 2.1.x (last updated 25th Mar 2019)
- [x] Fetches consumer group information directly from `__consumer_offsets` topic instead of querying single brokers for consumer group lags to ensure robustness in case of broker failures or leader elections
- [x] Kafka SASL/SSL support
- [x] Provides per consumergroup:topic lag metrics (removes a topic's metrics if a single partition metric in that topic couldn't be fetched)
- [x] Created to use in Kubernetes clusters (has liveness/readiness check and helm chart for easier setup)

## Setup

### Environment variables

| Variable name                      | Description                                                                                           | Default               |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------- | --------------------- |
| TELEMETRY_HOST                     | Host to listen on for the prometheus exporter                                                         | 0.0.0.0               |
| TELEMETRY_PORT                     | HTTP Port to listen on for the prometheus exporter                                                    | 8080                  |
| LOG_LEVEL                          | Log granularity (debug, info, warn, error, fatal, panic)                                              | info                  |
| VERSION                            | Application version (env variable is set in Dockerfile)                                               | (from Dockerfile)     |
| EXPORTER_IGNORE_SYSTEM_TOPICS      | Don't expose metrics about system topics (any topic names which are "\_\_" or "\_confluent" prefixed) | true                  |
| KAFKA_BROKERS                      | Array of broker addresses, delimited by comma (e. g. "kafka-1:9092, kafka-2:9092")                    | (No default)          |
| KAFKA_CONSUMER_OFFSETS_TOPIC_NAME  | Topic name of topic where kafka commits the consumer offsets                                          | \_\_consunmer_offsets |
| KAFKA_SASL_ENABLED                 | Bool to enable/disable SASL authentication (only SASL_PLAINTEXT is supported)                         | false                 |
| KAFKA_SASL_USE_HANDSHAKE           | Whether or not to send the Kafka SASL handshake first                                                 | true                  |
| KAFKA_SASL_USERNAME                | SASL Username                                                                                         | (No default)          |
| KAFKA_SASL_PASSWORD                | SASL Password                                                                                         | (No default)          |
| KAFKA_TLS_ENABLED                  | Whether or not to use TLS when connecting to the broker                                               | false                 |
| KAFKA_TLS_CA_FILE_PATH             | Path to the TLS CA file                                                                               | (No default)          |
| KAFKA_TLS_KEY_FILE_PATH            | Path to the TLS key file                                                                              | (No default)          |
| KAFKA_TLS_CERT_FILE_PATH           | Path to the TLS cert file                                                                             | (No default)          |
| KAFKA_TLS_INSECURE_SKIP_TLS_VERIFY | If true, TLS accepts any certificate presented by the server and any host name in that certificate.   | true                  |
| KAFKA_TLS_PASSPHRASE               | Passphrase to decrypt the TLS Key                                                                     | (No default)          |
| METRICS_PREFIX                     | A prefix for all exported prometheus metrics                                                          | kafka_minion          |

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

#### `kafka_minion_group_topic_lag{group, group_base_name, group_is_latest, group_version, topic}`

Number of messages the consumer group is behind for a given topic.

#### `kafka_minion_group_topic_partition_lag{group, group_base_name, group_is_latest, group_version, topic, partition}`

Number of messages the consumer group is behind for a given partition.

#### `kafka_minion_group_topic_partition_last_commit{group, group_base_name, group_is_latest, group_version, topic, partition}`

Timestamp when consumer group last commited an offset for a given partition.

#### `kafka_minion_topic_partition_high_water_mark{topic, partition}`

Latest known commited offset for this partition. This metric is being updated periodically and thus the actual high water mark may be ahead of this one.

#### `kafka_minion_topic_partition_low_water_mark{topic, partition}`

Oldest known commited offset for this partition. This metric is being updated periodically and thus the actual high water mark may be ahead of this one.

#### `kafka_minion_topic_partition_message_count{topic, partition}`

Number of messages for a given partition. Calculated by subtracting high water mark by low water mark. Thus this metric is likely to be invalid for compacting topics, but it still can be helpful to get an idea about the number of messages in that topic.

## How does it work

At a high level Kafka Minion fetches source data in two different ways.

1. **Consumer Group Data:** Since Kafka version 0.10 Zookeeper is no longer in charge of maintaining the consumer group offsets. Instead Kafka itself utilizes an internal Kafka topic called `__consumer_offsets`. Messages in that topic are binary and the protocol may change with broker upgrades. On each succesful client commit a message is created in that topic which contains the current offset for the according `groupId`, `topic` and `partition`.

   Additionally one can find group metadata messages in this topic.

2. **Broker requests:** Brokers are being queried to get topic metadata information, such as partition count, topic configuration, lowest & highest commited offset.

## FAQ

### Why did you create yet another kafka lag exporter?

1. As of writing the exporter there is no publicly available prometheus exporter which is lightweight, robust and supports Kafka v0.11 - v2.1+ (to my knowledge)

2. We are primarily interested in per consumergroup:topic lags. Some exporters export either only consumer group lags (of all topics alltogether) or they export only per partition metrics. While you can obviously aggregate those partition metrics in Grafana as well, it adds unnecessary complexity in Grafana dashboards. This exporter adds both.

3. In our environment developers occasionally replay data by creating a new consumer group. They do so by incrementing a trailing number (e. g. "sample-group-1" becomes "sample-group-2"). In order to setup a proper alerting based on increasing lags for all consumer groups in a cluster, we need to ignore those "outdated" consumer groups. In this illustration "sample-group-1" as it's not being used anymore. This exporter adds 3 labels on each exporter consumergroup:topic lag metric to make that possible: `group_base_name`, `group_version`, `group_is_latest`. The meaning of each label is explained in the section [Labels](#labels)

4. More (unique) metrics which are not available in other exporters. Likely some of these features are not desired in other exporters too.
