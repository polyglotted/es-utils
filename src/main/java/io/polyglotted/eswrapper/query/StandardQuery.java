package io.polyglotted.eswrapper.query;

import com.google.common.collect.ImmutableList;
import io.polyglotted.eswrapper.query.request.Expression;
import io.polyglotted.eswrapper.query.request.QueryHints;
import io.polyglotted.eswrapper.query.request.SimpleSort;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.copyOf;
import static io.polyglotted.eswrapper.query.request.QueryHints.hintsBuilder;
import static java.util.Arrays.asList;

@Accessors(fluent = true)
@RequiredArgsConstructor
public final class StandardQuery {
    public final ImmutableList<String> indices;
    public final ImmutableList<String> types;
    public final ImmutableList<String> fields;
    public final ImmutableList<Expression> expressions;
    public final ImmutableList<Expression> aggregates;
    public final ImmutableList<SimpleSort> sorts;
    public final QueryHints hints;
    public final Long scrollTimeInMillis;
    public final int offset;
    public final int size;

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
        private final List<SimpleSort> sorts = new ArrayList<>();
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

        public Builder sort(SimpleSort.Builder sortBuilder) {
            return sort(sortBuilder.build());
        }

        public Builder sort(SimpleSort... sorts) {
            this.sorts.addAll(asList(sorts));
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
               copyOf(aggregates), copyOf(sorts), hints, scrollTimeInMillis, offset, size);
        }
    }
}
