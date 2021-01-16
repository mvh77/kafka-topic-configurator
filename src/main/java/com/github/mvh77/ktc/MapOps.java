package com.github.mvh77.ktc;

import io.vavr.Tuple;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.Collection;

public class MapOps {

    public static Collection<AlterConfigOp> toConfigOps(Map<String, ConfigEntry> current, Map<String, String> updated) {
        var toSet1 = updated.flatMap((key, value) -> diff(value, current.get(key)).map(aco -> Tuple.of(key, aco)));
        var toSet2 = current.flatMap((key, value) -> diff(value, updated.get(key)).map(aco -> Tuple.of(key, aco)));
        return toSet1.merge(toSet2).values().sortBy(aco -> aco.configEntry().name()).toJavaList();
    }

    // current values will be updated if new value is available, is different from current or if new value is not available and current is not default
    private static Option<AlterConfigOp> diff(ConfigEntry current, Option<String> updated) {
        if (updated.isEmpty()) {
            if (current.isDefault() || current.source() != ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                return Option.none(); // no new value and already default or not dynamically set at topic level.
            else // set to default
                return Option.of(new AlterConfigOp(new ConfigEntry(current.name(), null), AlterConfigOp.OpType.DELETE));
        }
        return updated
                .flatMap(u -> {
                    if (u.equals(current.value()))
                        return Option.none(); // updated is current
                    else
                        return Option.of(new AlterConfigOp(new ConfigEntry(current.name(), u), AlterConfigOp.OpType.SET));
                });
    }

    // updated values will be set if recognized (option defined) and different from current value
    private static Option<AlterConfigOp> diff(String updated, Option<ConfigEntry> current) {
        return current.flatMap(c -> {
            if (c.value().equals(updated)) return Option.none();
            else return Option.of(new AlterConfigOp(new ConfigEntry(c.name(), updated), AlterConfigOp.OpType.SET));
        });
    }
}
