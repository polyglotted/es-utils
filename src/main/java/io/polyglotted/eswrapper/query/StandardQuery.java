package io.polyglotted.eswrapper.query;

import com.google.common.collect.ImmutableList;
import io.polyglotted.eswrapper.query.request.Expression;
import io.polyglotted.eswrapper.query.request.QueryHints;
import io.polyglotted.eswrapper.query.request.Sort;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.copyOf;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.query.request.QueryHints.hintsBuilder;
import static java.util.Arrays.asList;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class StandardQuery {
    public final ImmutableList<String> indices;
    public final ImmutableList<String> types;
    public final ImmutableList<String> fields;
    public final ImmutableList<Expression> expressions;
    public final ImmutableList<Expression> aggregates;
    public final ImmutableList<Sort> sorts;
    public final ImmutableList<Integer> substitutions;
    public final QueryHints queryHints;
    public final Long scrollTimeInMillis;
    public final int offset;
    public final int size;

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && GSON.toJson(this).equals(GSON.toJson(o));
    }

    @Override
    public int hashCode() {
        return Objects.hash(GSON.toJson(this));
    }

    public static Builder queryBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final List<String> indices = new ArrayList<>();
        private final List<String> types = new ArrayList<>();
        private final List<String> fields = new ArrayList<>();
        private final List<Expression> expressions = new ArrayList<>();
        private final List<Expression> aggregates = new ArrayList<>();
        private final List<Sort> sorts = new ArrayList<>();
        private final List<Integer> subs = new ArrayList<>();
        private QueryHints hints = hintsBuilder().build();
        private Long scrollTimeInMillis = null;
        private int offset = 0;
        private int size = 10;

        public Builder index(String... indices) {
            this.indices.addAll(asList(indices));
            return this;
        }

        public Builder type(String... types) {
            this.types.addAll(asList(types));
            return this;
        }

        public Builder field(String... fields) {
            this.fields.addAll(asList(fields));
            return this;
        }

        public Builder expression(Expression... expressions) {
            this.expressions.addAll(asList(expressions));
            return this;
        }

        public Builder aggregate(Expression... aggregates) {
            this.aggregates.addAll(asList(aggregates));
            return this;
        }

        public Builder sort(Sort.Builder sortBuilder) {
            return sort(sortBuilder.build());
        }

        public Builder sort(Sort... sorts) {
            this.sorts.addAll(asList(sorts));
            return this;
        }

        public Builder sub(Integer... subs) {
            this.subs.addAll(asList(subs));
            return this;
        }

        public Builder hints(QueryHints.Builder hintBuilder) {
            return hints(hintBuilder.build());
        }

        public Builder hints(QueryHints hints) {
            this.hints = hints;
            return this;
        }

        public StandardQuery build() {
            return new StandardQuery(copyOf(indices), copyOf(types), copyOf(fields), copyOf(expressions),
               copyOf(aggregates), copyOf(sorts), copyOf(subs), checkNotNull(hints), scrollTimeInMillis, offset, size);
        }
    }
}
