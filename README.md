# Kafka Minion (Alpha - Still in development)

Kafka minion is a prometheus exporter for Apache Kafka (v0.10.0+), created to expose consumer group lags on a per topic (rather than per partition) basis.

### Why did you create yet another kafka lag exporter?

It has been created because of two features which aren't provided by any of the other public kafka lag exporters:

- We are only interested in per consumergroup:topic lags. Some exporters export either only consumer group lags (of all topics alltogether) or they export per partition metrics

- In our environment developers occasionally replay data by creating a new consumer group. They do so by incrementing a trailing number (e. g. "sample-group-1" becomes "sample-group-2"). In order to setup a proper alerting based on increasing lags for all consumer groups in a cluster, we need to ignore those "outdated" consumer groups (in this case "sample-group-1" as it's not being used anymore). This exporter adds 3 labels on each exporter consumergroup:topic lag metric to make that possible: `consumer_group_base_name`, `consumer_group_version`, `is_latest_consumer_group`. The meaning of each label is explained in the section [Exposed Metrics](#exposed-metrics)

## Features

- [x] Supports Kafka 0.10.1.0 - 2.1.x (last updated 10th Feb 2019)
- [x] Kafka SASL/SSL support
- [x] Provides per consumergroup:topic lag metrics (removes a topic's metrics if a single partition metric in that topic couldn't be fetched)
- [x] Created to use in Kubernetes clusters (has liveness/readiness check and helm chart)

## Setup

### Environment variables

| Variable name                | Description                                                                                         | Default           |
| ---------------------------- | --------------------------------------------------------------------------------------------------- | ----------------- |
| PORT                         | HTTP Port to listen on for the prometheus exporter                                                  | 8080              |
| LOG_LEVEL                    | Log granularity (debug, info, warn, error, fatal, panic)                                            | info              |
| VERSION                      | Application version (env variable is set in Dockerfile)                                             | (from Dockerfile) |
| KAFKA_BROKERS                | Array of broker addresses, delimited by comma (e. g. "kafka-1:9092, kafka-2:9092")                  | (No default)      |
| SASL_ENABLED                 | Bool to enable/disable SASL authentication (only SASL_PLAINTEXT is supported)                       | false             |
| SASL_USE_HANDSHAKE           | Whether or not to send the Kafka SASL handshake first                                               | true              |
| SASL_USERNAME                | SASL Username                                                                                       | (No default)      |
| SASL_PASSWORD                | SASL Password                                                                                       | (No default)      |
| TLS_ENABLED                  | Whether or not to use TLS when connecting to the broker                                             | false             |
| TLS_CA_FILE_PATH             | Path to the TLS CA file                                                                             | (No default)      |
| TLS_KEY_FILE_PATH            | Path to the TLS key file                                                                            | (No default)      |
| TLS_CERT_FILE_PATH           | Path to the TLS cert file                                                                           | (No default)      |
| TLS_INSECURE_SKIP_TLS_VERIFY | If true, TLS accepts any certificate presented by the server and any host name in that certificate. | true              |
| METRICS_PREFIX               | A prefix for all exported prometheus metrics                                                        | kafka_minion      |

## Exposed metrics

| Metric name                           | Description                                                                                    |
| ------------------------------------- | ---------------------------------------------------------------------------------------------- |
| kafka_minion_consumer_group_topic_lag | Sum of all partition lags for a consumerGroup:topic combination                                |
| kafka_minion_topic_message_count      | Estimated message count (calculated by the sum of all partitions' high offset - lowest offset) |
| kafka_minion_topic_partition_count    | Number of partitions for a topic                                                               |

`kafka_minion_consumer_group_topic_lag` has four labels:

- consumer_group: Consumer Group ID
- consumer_group_base_name: The recognized name without "version"
- consumer_group_version: The parsed consumer group version
- is_latest_consumer_group: Indicates if this consumer group id has the highest version for the consumer group base name
