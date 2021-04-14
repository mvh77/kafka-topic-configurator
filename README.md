# Kafka Topic Configurator

Kafka topic configuration tool based on the Kafka client

Run:
```
java -jar kafka-topic-configurator-X.Y.Z.jar -bootstrap localhost:29092 -definitions config.yml -dryRun -incrementPartitionCount -extraProperties config.properties
```

Options:

```
java KafkaTopicConfiguratorMain [options...] arguments...
 -bootstrap VAL           : kafka bootstrap servers, in the form
                            host1:port1,host2:port2,...
 -definitions VAL         : topic definition files, in the form
                            config1.yml,config2.yml,...
 -dryRun                  : don't run any of the updates, just print the
                            current topics and the updates to execute (default: true)
 -extraProperties VAL     : extra .properties files for configuring the client,
                            in the form config1.properties,config2.properties,...
 -incrementPartitionCount : increment the partition count if changed (NOTE:
                            seldom safe for compacted topics) (default: false)
 -noReplication           : don't respect replication numbers for local testing
                            purposes (default: true)
 -removeTopics            : remove topics missing from the definition files
                            (default: true)
```

Configuration file syntax:

```yaml
topics:
  tmt.cdc.profiles.v1:
    partitions: 60
    replication: 1
    config:
      cleanup.policy: compact
      segment.ms: 600000 # 10 min
      min.cleanable.dirty.ratio: 0.1

  tmt.cmd.requests.v1:
    partitions: 60
    replication: 1
    config:
      cleanup.policy: delete
      retention.ms: 600000 # 10min
      segment.ms: 300000 # 5min
```

In addition to the `-extraProperties` flag (where properties can be imported from files) all environment variables starting with `KAFKA_CFG_` will be used as properties, i.e. something like `KAFKA_CFG_SECURITY_PROTOCOL` will become `security.protocol`.