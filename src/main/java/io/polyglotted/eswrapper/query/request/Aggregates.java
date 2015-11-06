package io.polyglotted.eswrapper.query.request;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterValues;
import static io.polyglotted.eswrapper.query.AggregationType.*;
import static io.polyglotted.eswrapper.query.request.Expression.ValueKey;
import static io.polyglotted.eswrapper.query.request.Expression.withMap;

public abstract class Aggregates {

    public static final String FieldKey = "field";
    public static final String SizeKey = "size";
    public static final String OrderKey = "order";
    public static final String AscKey = "asc";
    public static final String IntervalKey = "interval";
    public static final String FormatKey = "format";

    public static Expression max(String label, String field) {
        return withMap(Max.name(), label, of(FieldKey, field));
    }

    public static Expression min(String label, String field) {
        return withMap(Min.name(), label, of(FieldKey, field));
    }

    public static Expression sum(String label, String field) {
        return withMap(Sum.name(), label, of(FieldKey, field));
    }

    public static Expression avg(String label, String field) {
        return withMap(Avg.name(), label, of(FieldKey, field));
    }

    public static Expression count(String label, String field) {
        return withMap(Count.name(), label, of(FieldKey, field));
    }

    public static Expression term(String label, String field) {
        return term(label, field, 20);
    }

    public static Expression term(String label, String field, int size) {
        return withMap(Term.name(), label, of(FieldKey, field, SizeKey, size));
    }

    public static Expression term(String label, String field, int size, String order, boolean asc) {
        return withMap(Term.name(), label, of(FieldKey, field, SizeKey, size, OrderKey, order, AscKey, asc));
    }

    public static Expression stats(String label, String field) {
        return withMap(Statistics.name(), label, of(FieldKey, field));
    }

    public static Expression dateHistogram(String label, String field, String interval) {
        return dateHistogram(label, field, interval, "yyyy-MM-dd");
    }

    public static Expression dateHistogram(String label, String field, String interval, String format) {
        return withMap(DateHistogram.name(), label, of(FieldKey, field, IntervalKey, interval, FormatKey, format));
    }

    public static Builder termBuilder(String label, String field) {
        return termBuilder(label, field, 20);
    }

    public static Builder termBuilder(String label, String field, int size) {
        return termBuilder(label, field, size, null, false);
    }

    public static Builder termBuilder(String label, String field, int size, String order, boolean asc) {
        return aggsBuilder(true).oper(Term.name()).label(label).arg(FieldKey, field).arg(SizeKey, size)
           .arg(OrderKey, order).arg(AscKey, asc);
    }

    public static Builder maxBuilder(String label, String field) {
        return aggsBuilder(false).oper(Max.name()).label(label).arg(FieldKey, field);
    }

    public static Builder minBuilder(String label, String field) {
        return aggsBuilder(false).oper(Min.name()).label(label).arg(FieldKey, field);
    }

    public static Builder sumBuilder(String label, String field) {
        return aggsBuilder(false).oper(Sum.name()).label(label).arg(FieldKey, field);
    }

    public static Builder avgBuilder(String label, String field) {
        return aggsBuilder(false).oper(Avg.name()).label(label).arg(FieldKey, field);
    }

    public static Builder countBuilder(String label, String field) {
        return aggsBuilder(false).oper(Count.name()).label(label).arg(FieldKey, field);
    }

    public static Builder statsBuilder(String label, String field) {
        return aggsBuilder(false).oper(Statistics.name()).label(label).arg(FieldKey, field);
    }

    public static Builder dateHistogramBuilder(String label, String field, String interval) {
        return dateHistogramBuilder(label, field, interval, "yyyy-MM-dd");
    }

    public static Builder dateHistogramBuilder(String label, String field, String interval, String format) {
        return aggsBuilder(true).oper(DateHistogram.name()).label(label).arg(FieldKey, field)
           .arg(IntervalKey, interval).arg(FormatKey, format);
    }

    public static Builder filterAggBuilder(String label, Expression filter) {
        return aggsBuilder(true).oper(Filter.name()).label(label).arg(ValueKey, filter);
    }

    private static Builder aggsBuilder(boolean canHaveChildren) {
        return new Builder(canHaveChildren);
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String oper;
        private String label;
        private final boolean canHaveChildren;
        private final Map<String, Object> args = new LinkedHashMap<>();
        private final Map<String, Builder> children = new LinkedHashMap<>();

        public <T> Builder arg(String key, T value) {
            args.put(key, value);
            return this;
        }

        public Builder add(Builder child) {
            checkState(canHaveChildren, "aggregation does not support children");
            children.put(child.label, child);
            return this;
        }

        public Builder addAndGet(Builder child) {
            checkState(canHaveChildren, "aggregation does not support children");
            children.put(child.label, child);
            return child;
        }

        public Expression build() {
            return new Expression(oper, label, ImmutableMap.copyOf(filterValues(args, notNull())),
               ImmutableList.copyOf(transform(children.values(), Builder::build)));
        }
    }
}
