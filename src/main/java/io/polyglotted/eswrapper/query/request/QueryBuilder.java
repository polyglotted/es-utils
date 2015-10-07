package io.polyglotted.eswrapper.query.request;

import com.google.common.annotations.VisibleForTesting;
import io.polyglotted.eswrapper.query.AggregationType;
import io.polyglotted.eswrapper.query.ExpressionType;
import io.polyglotted.eswrapper.query.StandardQuery;
import io.polyglotted.eswrapper.query.StandardScroll;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.collect.Iterables.toArray;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.idsFilter;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

@Slf4j
public abstract class QueryBuilder {

    public static SearchRequest idRequest(String[] ids, Iterable<String> types, String... indices) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(ids.length)
           .query(constantScoreQuery(idsFilter(toStrArray(types)).ids(ids))).version(true);
        return new SearchRequest(indices).indicesOptions(lenientExpandOpen()).source(source);
    }

    public static SearchRequest aggregationToRequest(AbstractAggregationBuilder aggs, String... indices) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).query(matchAllQuery()).aggregation(aggs);
        return new SearchRequest(indices).indicesOptions(lenientExpandOpen()).source(source);
    }

    public static SearchScrollRequest scrollRequest(StandardScroll scroll) {
        return new SearchScrollRequest(scroll.id).scroll(timeValueMillis(scroll.scroll));
    }

    public static SearchRequest queryToRequest(StandardQuery query, FilterBuilder postFilter) {
        SearchRequest request = new SearchRequest(toStrArray(query.indices)).types(toStrArray(query.types));
        SearchSourceBuilder builder = new SearchSourceBuilder().version(true);
        setFields(builder, query.fields);
        setHints(request, builder, query.hints);
        setFilters(builder, query);
        setAggregations(builder, query);
        setOrder(builder, query);
        setScrollOrLimits(request, builder, query);
        builder.postFilter(postFilter);
        request.source(builder);
        return request;
    }

    @VisibleForTesting
    static void setFields(SearchSourceBuilder builder, List<String> fields) {
        if (fields.isEmpty()) return;
        builder.fields(fields);
        builder.fetchSource(true);
    }

    @VisibleForTesting
    static void setHints(SearchRequest request, SearchSourceBuilder builder, QueryHints hints) {
        request.indicesOptions(optionsFrom(hints.options));
        request.searchType(hints.type);
        request.preference(hints.preference);
        String[] routings = toStrArray(hints.routing);
        if (routings.length > 0) request.routing(routings);
        if (!hints.fetch) builder.fetchSource(false);
        builder.timeout(timeValueSeconds(hints.timeout));
    }

    @VisibleForTesting
    static void setFilters(SearchSourceBuilder builder, StandardQuery query) {
        FilterBuilder[] filters = ExpressionType.aggregateFilters(query.expressions);
        if (filters.length == 0)
            builder.query(matchAllQuery());
        else if (filters.length == 1)
            builder.query(constantScoreQuery(filters[0]));
        else
            builder.query(constantScoreQuery(andFilter(filters)));
    }

    @VisibleForTesting
    static void setAggregations(SearchSourceBuilder builder, StandardQuery query) {
        for (Expression aggr : query.aggregates)
            builder.aggregation(AggregationType.build(aggr));
    }

    @VisibleForTesting
    static void setOrder(SearchSourceBuilder builder, StandardQuery query) {
        for (SimpleSort sort : query.sorts)
            builder.sort(fieldSort(sort.field).order(SortOrder.valueOf(sort.order.name()))
               .sortMode(sort.mode.toMode()).unmappedType(sort.unmapped).missing(sort.missing));
    }

    private static void setScrollOrLimits(SearchRequest request, SearchSourceBuilder builder, StandardQuery query) {
        builder.size(query.size);
        if (query.scroll != null) {
            request.scroll(timeValueMillis(query.scroll));
        } else {
            builder.from(query.offset);
        }
    }

    @VisibleForTesting
    static IndicesOptions optionsFrom(String methodName) {
        try {
            Method method = IndicesOptions.class.getMethod(methodName);
            Object result = method.invoke(null);
            return IndicesOptions.class.cast(result);

        } catch (Exception e) {
            log.warn("unable to get indices option " + methodName);
            return lenientExpandOpen();
        }
    }

    private static String[] toStrArray(Iterable<String> iterable) {
        return toArray(iterable, String.class);
    }
}
