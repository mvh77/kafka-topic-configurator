package com.github.mvh77.ktc;

import io.vavr.collection.TreeMap;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
public class TopicDefinition {

    private int partitions = 1;
    private int replication = 1;
    private Map<String, String> config = Map.of();

    io.vavr.collection.Map<String, String> getConfigMap() {
        return TreeMap.ofAll(config);
    }
}
