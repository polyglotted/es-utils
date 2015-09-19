package io.polyglotted.eswrapper.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.query.request.Expression;
import io.polyglotted.eswrapper.query.response.Aggregation;
import io.polyglotted.eswrapper.query.response.Bucket;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.util.Map;

import static io.polyglotted.eswrapper.query.request.Aggregates.*;
import static io.polyglotted.eswrapper.query.response.Aggregation.aggregationBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum AggregationType {
    Max(false, false) {
        @Override
        AbstractAggregationBuilder buildFrom(Expression expr) {
            return max(expr.label).field(expr.stringArg(FieldKey));
        }

        @Override
        Aggregation.Builder getFrom(Expression expr, Aggregations aggregations) {
            org.elasticsearch.search.aggregations.metrics.max.Max max = aggregations.get(expr.label);
            return aggregationBuilder().label(expr.label)
               .type(AggregationType.Max).value(name(), max.getValue());
        }
    }, 
    Min(false, false) {
        @Override
        AbstractAggregationBuilder buildFrom(Expression expr) {
            return min(expr.label).field(expr.stringArg(FieldKey));
        }

        @Override
        Aggregation.Builder getFrom(Expression expr, Aggregations aggregations) {
            org.elasticsearch.search.aggregations.metrics.min.Min min = aggregations.get(expr.label);
            return aggregationBuilder().label(expr.label)
               .type(AggregationType.Min).value(name(), min.getValue());
        }
    }, 
    Sum(false, false) {
        @Override
        AbstractAggregationBuilder buildFrom(Expression expr) {
            return sum(expr.label).field(expr.stringArg(FieldKey));
        }

        @Override
        Aggregation.Builder getFrom(Expression expr, Aggregations aggregations) {
            org.elasticsearch.search.aggregations.metrics.sum.Sum sum = aggregations.get(expr.label);
            return aggregationBuilder().label(expr.label)
               .type(AggregationType.Sum).value(name(), sum.getValue());
        }
    }, 
    Avg(false, false) {
        @Override
        AbstractAggregationBuilder buildFrom(Expression expr) {
            return avg(expr.label).field(expr.stringArg(FieldKey));
        }

        @Override
        Aggregation.Builder getFrom(Expression expr, Aggregations aggregations) {
            org.elasticsearch.search.aggregations.metrics.avg.Avg avg = aggregations.get(expr.label);
            return aggregationBuilder().label(expr.label)
               .type(AggregationType.Avg).value(name(), avg.getValue());
        }
    }, 
    Count(false, false) {
        @Override
        AbstractAggregationBuilder buildFrom(Expression expr) {
            return count(expr.label).field(expr.stringArg(FieldKey));
        }

        @Override
        Aggregation.Builder getFrom(Expression expr, Aggregations aggregations) {
            org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount count = aggregations.get(expr.label);
            return aggregationBuilder().label(expr.label)
               .type(AggregationType.Count).value(name(), count.getValue());
        }
    },
    Term(true, true) {
        @Override
        @SuppressWarnings("unchecked")
        public <T> T valueFrom(Map<String, Object> valueMap, Iterable<Bucket> buckets) {
            return (T) ImmutableList.copyOf(buckets);
        }

        @Override
        AbstractAggregationBuilder buildFrom(Expression expr) {
            TermsBuilder termsBuilder = terms(expr.label)
               .field(expr.stringArg(FieldKey))
               .size(expr.intArg(SizeKey, 20))
               .order(orderFrom(expr))
               .showTermDocCountError(true);
            for (Expression child : expr.children) {
                termsBuilder.subAggregation(build(child));
            }
            return termsBuilder;
        }

        @Override
        Aggregation.Builder getFrom(Expression expr, Aggregations aggregations) {
            Terms terms = aggregations.get(expr.label);
            Aggregation.Builder builder = aggregationBuilder().label(expr.label)
               .type(AggregationType.Term).param("docCountError", terms.getDocCountError())
               .param("sumOfOtherDocs", terms.getSumOfOtherDocCounts());

            for (Terms.Bucket bucket : terms.getBuckets()) {
                Bucket.Builder bucketBuilder = builder.bucketBuilder().key(bucket.getKey())
                   .keyValue(bucket.getKey()).docCount(bucket.getDocCount())
                   .docCountError(bucket.getDocCountError());

                for (Expression child : expr.children) {
                    bucketBuilder.aggregation(getInternal(child, bucket.getAggregations()));
                }
            }
            return builder;
        }
    },
    Statistics(false, true) {
        @Override
        @SuppressWarnings("unchecked")
        public <T> T valueFrom(Map<String, Object> valueMap, Iterable<Bucket> buckets) {
            return (T) ImmutableMap.copyOf(valueMap);
        }

        @Override
        AbstractAggregationBuilder buildFrom(Expression expr) {
            return stats(expr.label).field(expr.stringArg(FieldKey));
        }

        @Override
        Aggregation.Builder getFrom(Expression expr, Aggregations aggregations) {
            Stats stats = aggregations.get(expr.label);

            return aggregationBuilder().label(expr.label).type(AggregationType.Statistics)
               .value(Count.name(), stats.getCount()).value(Max.name(), stats.getMax())
               .value(Min.name(), stats.getMin()).value(Avg.name(), stats.getAvg())
               .value(Sum.name(), stats.getSum());
        }
    },
    DateHistogram(true, true) {
        @Override
        @SuppressWarnings("unchecked")
        public <T> T valueFrom(Map<String, Object> valueMap, Iterable<Bucket> buckets) {
            return (T) ImmutableList.copyOf(buckets);
        }

        @Override
        AbstractAggregationBuilder buildFrom(Expression expr) {
            DateHistogramBuilder builder = dateHistogram(expr.label).field(expr.stringArg(FieldKey))
               .interval(new DateHistogram.Interval(expr.stringArg(IntervalKey)))
               .format(expr.stringArg(FormatKey));
            for (Expression child : expr.children) {
                builder.subAggregation(build(child));
            }
            return builder;
        }

        @Override
        Aggregation.Builder getFrom(Expression expr, Aggregations aggregations) {
            DateHistogram dateHistogram = aggregations.get(expr.label);
            Aggregation.Builder builder = aggregationBuilder().label(expr.label)
               .type(AggregationType.DateHistogram);

            for (DateHistogram.Bucket bucket : dateHistogram.getBuckets()) {
                Bucket.Builder bucketBuilder = builder.bucketBuilder().key(bucket.getKey())
                   .keyValue(bucket.getKeyAsDate().getMillis()).docCount(bucket.getDocCount());

                for (Expression child : expr.children) {
                    bucketBuilder.aggregation(getInternal(child, bucket.getAggregations()));
                }
            }
            return builder;
        }
    };

    public final boolean hasBuckets;
    public final boolean isMultiValue;

    @SuppressWarnings("unchecked")
    public <T> T valueFrom(Map<String, Object> valueMap, Iterable<Bucket> buckets) {
        return (T) valueMap.get(name());
    }

    public static AbstractAggregationBuilder build(Expression expr) {
        return valueOf(expr.operation).buildFrom(expr);
    }

    abstract AbstractAggregationBuilder buildFrom(Expression expr);

    public static Aggregation get(Expression expr, Aggregations aggregations) {
        return getInternal(expr, aggregations).build();
    }

    private static Aggregation.Builder getInternal(Expression expr, Aggregations aggregations) {
        return valueOf(expr.operation).getFrom(expr, aggregations);
    }

    abstract Aggregation.Builder getFrom(Expression expr, Aggregations aggregations);

    private static Terms.Order orderFrom(Expression expr) {
        String orderVal = expr.stringArg(OrderKey);
        boolean asc = expr.boolArg(AscKey);

        if (orderVal == null) {
            return null;
        } else if ("count".equalsIgnoreCase(orderVal)) {
            return Terms.Order.count(asc);
        } else if ("term".equalsIgnoreCase(orderVal)) {
            return Terms.Order.term(asc);
        } else {
            return Terms.Order.aggregation(orderVal, asc);
        }
    }
}
