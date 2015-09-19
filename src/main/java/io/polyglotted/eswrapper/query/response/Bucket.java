package io.polyglotted.eswrapper.query.response;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.transform;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString(doNotUseGetters = true, includeFieldNames = false, of = {"key", "docCount", "aggregations"})
public final class Bucket {
    public final String key;
    public final Object keyValue;
    public final long docCount;
    public final long docCountError;
    public final ImmutableList<Aggregation> aggregations;

    @SuppressWarnings("unchecked")
    public <T> T keyValue() {
        return (T) keyValue;
    }

    public boolean hasAggregations() {
        return aggregations.size() > 0;
    }

    public static Builder bucketBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class Builder {
        private String key;
        private Object keyValue;
        private long docCount;
        private long docCountError;
        private final List<Aggregation.Builder> builders = new ArrayList<>();

        public Builder aggregation(Aggregation.Builder builder) {
            this.builders.add(builder);
            return this;
        }

        public Bucket build() {
            Iterable<Aggregation> aggregations = transform(builders, Aggregation.Builder::build);
            return new Bucket(key, keyValue, docCount, docCountError, ImmutableList.copyOf(aggregations));
        }
    }
}
