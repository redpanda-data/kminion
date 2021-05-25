# End-To-End Monitoring

This page describes the end-to-end monitoring feature in kminion, how it works, and what metrics it provides.

## Motivation
> What is the issue? Why did we build this feature?

We can (and do!) monitor metrics like CPU usage, free disk space, or even consumer group lag.
However those metrics don't give us a good idea of the performance characteristics an actual, real-world, client can expect from the cluster.

With the "classic" metrics lots of questions go unanswered: 
- Can a client even produce messages to the cluster in general? 
- Does the cluster still give a an acceptable performance for producing messages, committing offsets, and consuming messages? 
- What if only a subset of the brokers is somehow unable to handle requests?

## Approach & Implementation
> How do we solve those issues? How does the feature work?

The most reliably way to get real-world performance and availability metrics is to just actually run a producer/consumer ourselves - which is exactly what the end-to-end monitoring feature does!

## High Level Overview
In order to determine if the cluster is fully operational and its performance is within acceptable limits, kminion continously produces and consumes messages to/from the cluster. That way we can measure things like ack-latency, commit-latency, and roundtrip-time.

Kminion creates and manages its own topic for the end-to-end test messages. The name of the topic can be configured.

**The first step** is to create a message and send it to the cluster.
- Every produced message is added to an internal tracker so we can recognize messages being "lost" (messages that don't arrive back at the consumer within the configured time span).

**The second step** is to continously consume the topic.
- As each message arrives, we calculate its roundtrip time (time from the point the message was created, until kminion received it again)
- Consumer group offsets are committed periodically, while also recording the time each commit takes.

### Topic Management
The topic kminion uses is created and managed completely automatically (the topic name can be configured though).

Kminion continously verifies the topic and fixes issues automatically:
- Add partitions to the topic so it has at least as many partitions as there are brokers.
- Will reassign partitions to ensure every broker leads at least one partition, and that all partitions' replicas are distributed evenly across the brokers. (Preferably assigns so partitionID and leader brokerID matches)


### Consumer Group Management
On startup each kminion instance generates a unique identifier (UUID) used to create its own consumer group - using the shared prefix from the config.

That is neccesary because:
- Offsets must not be shared among multiple instances.
- Each instance must always consume **all** partitions of the topic.

The instances' UUID is also embedded in every message, so each instance can easily filter out messages it didn't produce.
That's why it is perfectly fine to run multiple kminion instances against the same cluster, using the same topic.

Kminion also monitors and deletes consumer groups that use its configured prefix.
That way, when an instance exits/restarts, previous consumer groups will be cleaned up quickly (check happens every 20s).


## Available Metrics
The end-to-end monitoring feature exports the following metrics.

### Counters
| Name | Description |
| --- | --- |
| `kminion_end_to_end_messages_produced_total ` | Messages kminion *tried* to send |
| `kminion_end_to_end_messages_acked_total ` | Messages actually sent and acknowledged by the cluster |
| `kminion_end_to_end_messages_received_total ` | Number of messages received (only counts those that match, i.e. that this instance actually produced itself) |
| `kminion_end_to_end_commits_total` | Number of successful offset commits |


### Histograms
| Name | Description |
| --- | --- |
| `kminion_end_to_end_produce_latency_seconds ` | Duration until a the cluster acknowledged a message.  |
| `kminion_end_to_end_commit_latency_seconds` | Duration of offset commits. Has a label for coordinator brokerID that answered the commit request |
| `kminion_end_to_end_roundtrip_latency_seconds ` | Duration from creation of a message, until it was received/consumed agagin. |



## Config Properties
All config properties related to this feature are located in `minion.endToEnd`.

```yaml
  endToEnd:
    enabled: true
    probeInterval: 800ms # how often to send end-to-end test messages
    topicManagement:
      # You can disable topic management, without disabling the testing feature.
      # Only makes sense if you have multiple kminion instances, and for some reason only want one of them to create/configure the topic.
      # It is strongly reccommended to leave this enabled. 
      enabled: true

      # Name of the topic kminion uses to send its test messages
      # You do *not* need to change this if you are running multiple kminion instances on the same cluster.
      # Different instances are perfectly fine with sharing the same topic!
      name: kminion-end-to-end

      # How often kminion checks its topic to validate configuration, partition count, and partition assignments
      reconciliationInterval: 10m

      # Useful for monitoring the performance of acks (if >1 this is best combined with 'producer.requiredAcks' set to 'all')
      replicationFactor: 1

      # Rarely makes sense to change this, but maybe if you want some sort of cheap load test?
      partitionsPerBroker: 1

    producer:
      # This defines the maximum time to wait for an ack response after producing a message,
      # and the upper bound for histogram buckets in "produce_latency_seconds"
      ackSla: 5s
      # Can be to "all" (default) so kafka only reports an end-to-end test message as acknowledged if
      # the message was written to all in-sync replicas of the partition.
      # Or can be set to "leader" to only require to have written the message to its log.
      requiredAcks: all

    consumer:
      # Prefix kminion uses when creating its consumer groups. Current kminion instance id will be appended automatically
      groupIdPrefix: kminion-end-to-end
      # Defines the time limit beyond which a message is considered "lost" (failed the roundtrip),
      # also used as the upper bound for histogram buckets in "roundtrip_latency"
      roundtripSla: 20s

      # Maximum time an offset commit is allowed to take before considering it failed,
      # also used as the upper bound for histogram buckets in "commit_latency_seconds"
      commitSla: 10s
```

