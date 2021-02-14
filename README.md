# Prometheus Exporter for Apache Kafka - KMinion

KMinion (previously known as Kafka Minion) is a feature rich and flexible Prometheus Exporter to monitor your Apache
Kafka cluster. All valuable information that are accessible via the Kafka protocol are supposed to be accessible using
KMinion.

## Features

- **Kafka versions:** Supports all Kafka versions v0.11+
- **Supported SASL mechanisms:** plain, scram-sha-256/512, gssapi/kerberos
- **TLS support:** TLS is supported, regardless whether you need mTLS, a custom CA, encrypted keys or just the trusted
  root certs
- **Consumer Group Lags:** Number of messages a consumer group is lagging behind the latest offset
- **Log dir sizes:** Metric for log dir sizes either grouped by broker or by topic
- **Broker info:** Metric for each broker with its address, broker id, controller and rack id
- **Configurable granularity:** Export metrics (e.g. consumer group lags) either per partition or per topic. This helps
  to reduce the number of exported metric series
- **Configurable targets:** You can configure what topics or groups you'd like to export using regex expressions
- **Multiple config parsers:** It's possible to configure KMinion using YAML, Environment variables or a mix of both

## Getting started

### Docker image

TODO

### Helm chart

TODO

### Configuration

All options in KMinion can be configured via YAML or environment variables. Configuring some options via YAML and some
via environment variables is also possible. Environment variables take precedence in this case. You can find the
reference config with additional documentation in [/docs/reference-config.yaml](/docs/reference-config.yaml).

### Testing locally

This repo contains a docker-compose file that you can run on your machine. It will spin up a Kafka & ZooKeeper cluster
and starts KMinion on port 8080 which is exposed to your host machine:

```shell
# 1. Clone this repo
# 2. Browse to the repo's root directory and run:
docker-compose up
```