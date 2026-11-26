package io.github.azabaga.offer.domain;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

final class EntityIds {
    private EntityIds() {
    }

    static <T> List<T> uniqueList(List<T> items, Function<T, String> identify, String entityName) {
        List<T> copy = List.copyOf(items);
        Set<String> ids = new HashSet<>();
        for (T item : copy) {
            String id = identify.apply(item);
            if (!ids.add(id)) {
                throw new IllegalArgumentException("Duplicate " + entityName + " ID: " + id);
            }
        }
        return copy;
    }
}
