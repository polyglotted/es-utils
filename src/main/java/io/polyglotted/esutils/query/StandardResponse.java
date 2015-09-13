package io.polyglotted.esutils.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.polyglotted.esutils.query.response.Aggregation;
import io.polyglotted.esutils.query.response.ResponseHeader;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@RequiredArgsConstructor
public final class StandardResponse {
    public final ResponseHeader header;
    public final ImmutableList<Object> results;
    public final ImmutableList<Aggregation> aggregations;

    public <T> Iterable<T> resultsAs(Class<? extends T> tClass) {
        return Iterables.transform(results, tClass::cast);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private ResponseHeader header = null;
        private final List<Object> results = new ArrayList<>();
        private List<Aggregation> aggregations = new ArrayList<>();

        public Builder results(Iterable<?> objects) {
            Iterables.addAll(results, objects);
            return this;
        }

        public StandardResponse build() {
            return new StandardResponse(checkNotNull(header, "header cannot be null"),
               ImmutableList.copyOf(results), ImmutableList.copyOf(aggregations));
        }
    }
}
