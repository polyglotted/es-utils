package io.polyglotted.eswrapper.query.response;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString(includeFieldNames = false, doNotUseGetters = true, of = {"key", "count", "aggregations"})
public final class Bucket {
    public final String key;
    public final Object value;
    public final long count;
    public final long errors;
    public final ImmutableList<Aggregation> aggregations;

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && GSON.toJson(this).equals(GSON.toJson(o));
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, count, errors, aggregations);
    }

    @SuppressWarnings("unchecked")
    public <T> T keyValue() {
        return (T) value;
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
