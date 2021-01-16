package com.github.mvh77.ktc;

import lombok.Data;

import java.util.Collections;
import java.util.Map;

@Data
public class FileTopics {

    private Map<String, TopicDefinition> topics = Collections.emptyMap();

    public void setTopics(Map<String, TopicDefinition> topics) {
        if (topics == null) { // might be called with null by snakeyaml
            this.topics = Collections.emptyMap();
        } else {
            this.topics = topics;
        }
    }
}
