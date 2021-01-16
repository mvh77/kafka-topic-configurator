package com.github.mvh77.ktc;

import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CustomAdminClient {

    private final AdminClient adminClient;

    CustomAdminClient(String bootstrap) {
        Map<String, Object> properties = HashMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap, AdminClientConfig.RETRIES_CONFIG, 5);
        this.adminClient = AdminClient.create(properties.toJavaMap());
    }

    void close() {
        adminClient.close();
    }

    CompletableFuture<HashMap<TopicDescription, Set<ConfigEntry>>> getTotalDescription() {
        return getTopicDescriptions()
                .thenCompose(tds -> {
                    var futures = tds
                            .map(td -> getConfigEntries(td.name()).thenApply(s -> Tuple.of(td, s)))
                            .toJavaList();
                    return CompletableFutures
                            .combined(futures, HashMap.collector());
                });
    }

    CompletableFuture<Void> doCreateTopics(Seq<NewTopic> newTopics, boolean dryRun) {
        return toCompletableFuture(adminClient.createTopics(newTopics.asJava(), new CreateTopicsOptions().validateOnly(dryRun)).all());
    }

    CompletableFuture<Void> doUpdateTopics(Map<ConfigResource, Collection<AlterConfigOp>> mods, boolean dryRun) {
        return toCompletableFuture(adminClient.incrementalAlterConfigs(mods.toJavaMap(), new AlterConfigsOptions().validateOnly(dryRun)).all());
    }

    CompletableFuture<Void> doUpdatePartitionCount(Map<String, Integer> topicToNewPartitionCount) {
        return toCompletableFuture(adminClient.createPartitions(topicToNewPartitionCount.mapValues(NewPartitions::increaseTo).toJavaMap()).all());
    }

    CompletableFuture<Void> doDeleteTopics(Set<String> deletedTopics) {
        return toCompletableFuture(adminClient.deleteTopics(deletedTopics.toJavaSet()).all());
    }

    // ------------------------------------------------------------------------

    private CompletableFuture<Set<ConfigEntry>> getConfigEntries(String topic) {
        return toCompletableFuture(adminClient.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, topic))).all())
                .thenApply(map -> map.values().stream().flatMap(c -> c.entries().stream()))
                .thenApply(HashSet::ofAll);
    }

    private CompletableFuture<Set<TopicDescription>> getTopicDescriptions() {
        return topics()
                .thenCompose(topics -> toCompletableFuture(adminClient.describeTopics(topics.toJavaSet()).all()))
                .thenApply(map -> HashSet.ofAll(map.values()));
    }

    private CompletableFuture<Set<String>> topics() {
        return toCompletableFuture(adminClient.listTopics().names()
                .thenApply(HashSet::ofAll)
                .thenApply(hs -> hs.filter(name -> !name.startsWith("_"))));

    }

    private <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> future) {
        CompletableFuture<T> ret = new CompletableFuture<>();
        future.whenComplete((t, throwable) -> {
            if (throwable != null) ret.completeExceptionally(throwable);
            else ret.completeAsync(() -> t);
        });
        return ret;
    }
}
