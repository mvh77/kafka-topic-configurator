package com.github.mvh77.ktc;

import com.github.javactic.Accumulation;
import com.github.javactic.One;
import com.github.javactic.Or;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import io.vavr.collection.Vector;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KafkaTopicConfigurator {

    public void execute(String bootstrap, String definitions, String extraProperties, boolean dryRun, boolean removeTopics, boolean noReplication) {
        CustomAdminClient client = new CustomAdminClient(bootstrap, extraProperties);
        client.getTotalDescription()
                .whenComplete((topics, error) -> {
                    if (topics != null) {
                        printCurrentTopicInfo(topics);
                        Map<String, TopicDefinition> targetTopics = parseInput(definitions, noReplication);
                        var currentTopics = topics.mapKeys(TopicDescription::name);
                        var newTopics = getTopicsToCreate(currentTopics, targetTopics);
                        createTopics(client, newTopics, dryRun);
                        var updatedTopics = getTopicsToUpdate(currentTopics, targetTopics);
                        updateTopics(client, updatedTopics, dryRun);
                        var updatedTopicCounts = getPartitionCountsToUpdate(topics.keySet(), targetTopics);
                        updateTopicCount(client, updatedTopicCounts, dryRun);
                        var deletedTopics = currentTopics.keySet().removeAll(targetTopics.keySet());
                        deleteTopics(client, deletedTopics, removeTopics);
                    }
                    if (error != null) {
                        errorPrintln("Error retrieving currently configured topics with", error);
                    }
                    client.close();
                })
                .join();
    }

    private Map<String, TopicDefinition> parseInput(String definitions, boolean noReplication) {
        String[] filesArray = definitions.split(",");
        Vector<String> files = Vector.of(filesArray).filter(s -> !s.isEmpty());
        Yaml yaml = new Yaml(new Constructor(FileTopics.class));
        Vector<Or<FileTopics, One<String>>> projectTopics = files
                .map(file ->
                        Or.from(Try.of(() -> {
                            InputStream is = new FileInputStream(file);
                            return yaml.<FileTopics>load(is);
                        })).badMap(th -> "could not parse configuration file " + file + " with exception " + th).accumulating());
        HashMap<String, TopicDefinition> result = Accumulation
                .combined(projectTopics)
                .forBad(ths -> ths.forEach(this::errorPrintln))
                .getOrElse(Vector.empty())
                .foldLeft(HashMap.empty(), (map, fileTopics) -> map.merge(HashMap.ofAll(fileTopics.getTopics())));
        if (noReplication) {
            result.forEach((k, v) -> v.setReplication(1));
        }
        return result;
    }

    private void printCurrentTopicInfo(Map<TopicDescription, Set<ConfigEntry>> definedTopics) {
        println("------------------------------------------------------------------------");
        println("--                  - CURRENTLY CONFIGURED TOPICS -                   --");
        println("------------------------------------------------------------------------");
        definedTopics.forEach((td, ces) -> {
            println(td.name() + " (" + td.partitions().size() + " partitions)");
            ces.toSortedSet(Comparator.comparing(ConfigEntry::name))
                    .forEach(ce -> {
                        var defaultIndicator = ce.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG ? " (*)" : "";
                        println("  " + ce.name() + ": " + ce.value() + defaultIndicator);
                    });
        });
    }

    Seq<NewTopic> getTopicsToCreate(Map<String, Set<ConfigEntry>> current, Map<String, TopicDefinition> targetTopics) {
        Map<String, TopicDefinition> newTopics = targetTopics.removeAll(current.keySet());
        return newTopics
                .map(t2 -> new NewTopic(t2._1, t2._2.getPartitions(), (short) t2._2.getReplication()).configs(t2._2.getConfig()))
                .sortBy(NewTopic::name);
    }

    private void createTopics(CustomAdminClient client, Seq<NewTopic> newTopics, boolean dryRun) {
        println("------------------------------------------------------------------------");
        println("--                        - TOPICS TO CREATE -                        --");
        println("------------------------------------------------------------------------");
        newTopics.forEach(nt -> println(nt.name()));
        client.doCreateTopics(newTopics, dryRun)
                .orTimeout(5, TimeUnit.SECONDS)
                .whenComplete((nil, th) -> {
                    if (th != null) {
                        errorPrintln("Could not create new topics with", th);
                    }
                })
                .join();
        println("");
    }

    Map<ConfigResource, Collection<AlterConfigOp>> getTopicsToUpdate(Map<String, Set<ConfigEntry>> current, Map<String, TopicDefinition> targetTopics) {
        Set<String> toUpdate = targetTopics.keySet().retainAll(current.keySet());
        return toUpdate
                .toMap(topic -> {
                    var updates = MapOps.toConfigOps(
                            current.apply(topic).toMap(ce -> Tuple.of(ce.name(), ce)),
                            targetTopics.apply(topic).getConfigMap());
                    return Tuple.of(new ConfigResource(ConfigResource.Type.TOPIC, topic), updates);
                })
                .filterValues(c -> !c.isEmpty());
    }

    private void updateTopics(CustomAdminClient client, Map<ConfigResource, Collection<AlterConfigOp>> toUpdate, boolean dryRun) {
        println("------------------------------------------------------------------------");
        println("--                        - TOPICS TO UPDATE -                        --");
        println("------------------------------------------------------------------------");
        toUpdate.forEach((key, value) -> {
            println(key.name());
            value.forEach(aco -> println("  " + aco.configEntry().name() + ": " + aco.configEntry().value() + " (" + aco.opType() + ")"));
        });
        client.doUpdateTopics(toUpdate, dryRun)
                .orTimeout(5, TimeUnit.SECONDS)
                .whenComplete((nil, th) -> {
                    if (th != null) {
                        errorPrintln("Could not update topics with", th);
                    }
                })
                .join();
        println("");
    }

    private Map<String, Tuple2<Integer, Integer>> getPartitionCountsToUpdate(Set<TopicDescription> currentTopics, Map<String, TopicDefinition> targetTopics) {
        return currentTopics
                .flatMap(td -> targetTopics.get(td.name())
                        .map(TopicDefinition::getPartitions)
                        .flatMap(i -> i > td.partitions().size() ? Option.of(Tuple.of(td.name(), Tuple.of(td.partitions().size(), i))) : Option.none()))
                .toMap(Function.identity());
    }

    private void updateTopicCount(CustomAdminClient client, Map<String, Tuple2<Integer, Integer>> updatedTopicCounts, boolean dryRun) {
        println("------------------------------------------------------------------------");
        println("--                 - PARTITION COUNTS TO INCREASE -                   --");
        println("------------------------------------------------------------------------");
        updatedTopicCounts.forEach((topic, count) -> println(topic + " " + count._1 + " -> " + count._2));
        if (!dryRun) {
            client.doUpdatePartitionCount(updatedTopicCounts.mapValues(t2 -> t2._2));
        }
    }

    private void deleteTopics(CustomAdminClient client, Set<String> topics, boolean removeTopics) {
        println("------------------------------------------------------------------------");
        println("--                        - TOPICS TO DELETE -                        --");
        println("------------------------------------------------------------------------");
        topics.forEach(topic -> println("  " + topic));
        if (removeTopics) {
            client.doDeleteTopics(topics);
        }
    }

    private void println(String s) {
        System.out.println(s);
    }

    private void errorPrintln(String s, Throwable... args) {
        var messages = Arrays.stream(args).map(Throwable::getMessage).collect(Collectors.joining("\n"));
        System.err.println(s);
        if (!messages.isBlank()) {
            System.out.println(messages);
        }
    }
}
