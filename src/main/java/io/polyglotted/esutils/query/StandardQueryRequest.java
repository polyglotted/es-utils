package io.polyglotted.esutils.query;

import com.google.common.collect.ImmutableList;
import io.polyglotted.esutils.query.request.Aggregates;
import io.polyglotted.esutils.query.request.Expression;
import io.polyglotted.esutils.query.request.QueryHints;
import io.polyglotted.esutils.query.request.SimpleSort;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@RequiredArgsConstructor
public final class StandardQueryRequest {
    public final ImmutableList<String> indices;
    public final ImmutableList<String> types;
    public final ImmutableList<Expression> expressions;
    public final ImmutableList<Aggregates> aggregates;
    public final ImmutableList<SimpleSort> sorts;
    public final QueryHints hints;
    public final Long scrollTimeInMillis;
    public final int offset;
    public final int size;
}
