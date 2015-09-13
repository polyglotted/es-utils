package io.polyglotted.esutils.query.response;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.esutils.query.AggregationType;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;


@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString(doNotUseGetters = true, includeFieldNames = false, of = {"label", "type", "value"})
public final class Aggregation {
    public final String label;
    public final AggregationType type;
    public final Object value;
    public final ImmutableMap<String, Object> paramsMap;

    public boolean hasBuckets() {
        return type.hasBuckets;
    }

    @SuppressWarnings("unchecked")
    public Iterable<Bucket> buckets() {
        checkState(hasBuckets(), type.name() + " does not support buckets");
        return (Iterable<Bucket>) value;
    }

    public <T> T param(String name, Class<T> tClass) {
        return tClass.cast(paramsMap.get(name));
    }

    public long longValue(String name) {
        return value(name, Long.class);
    }

    public double doubleValue(String name) {
        return value(name, Double.class);
    }

    @SuppressWarnings("unchecked")
    public <T> T value(String name, Class<T> tClass) {
        return (value instanceof Map) ? tClass.cast(((Map) value).get(name)) : tClass.cast(value);
    }

    @SuppressWarnings("unchecked")
    public Iterable<Entry<String, Object>> valueIterable() {
        return (value instanceof Map) ? ((Map<String, Object>) value).entrySet()
                : Collections.singletonList(new SimpleEntry("value", value));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String label;
        private AggregationType type;
        private final java.util.Map<String, Object> valueMap = new LinkedHashMap<>();
        private final java.util.Map<String, Object> paramsMap = new LinkedHashMap<>();
        private final java.util.List<Bucket.Builder> builders = new ArrayList<>();

        public Builder value(String key, Object value) {
            valueMap.put(key, value);
            return this;
        }

        public Builder param(String key, Object value) {
            paramsMap.put(key, value);
            return this;
        }

        public Bucket.Builder bucketBuilder() {
            Bucket.Builder builder = Bucket.builder();
            this.builders.add(builder);
            return builder;
        }

        public Aggregation build() {
            Iterable<Bucket> buckets = transform(builders, Bucket.Builder::build);
            return new Aggregation(checkNotNull(label, "label cannot be null"), checkNotNull(type,
                    "type cannot be null"), type.valueFrom(valueMap, buckets), ImmutableMap.copyOf(paramsMap));
        }
    }
}
