package io.polyglotted.eswrapper.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.polyglotted.pgmodel.search.query.Expression;
import io.polyglotted.pgmodel.search.query.QueryHints;
import io.polyglotted.pgmodel.search.query.Sort;
import io.polyglotted.pgmodel.search.query.StandardQuery;
import io.polyglotted.pgmodel.search.query.StandardScroll;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static com.google.common.collect.Iterables.toArray;
import static io.polyglotted.eswrapper.ElasticConstants.PARENT_META;
import static io.polyglotted.eswrapper.query.ModelQueryUtil.orderOf;
import static io.polyglotted.eswrapper.query.ModelQueryUtil.toOptions;
import static java.util.Collections.singleton;
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
        SearchSourceBuilder source = new SearchSourceBuilder().size(ids.length * 10)
           .query(constantScoreQuery(idsFilter(toStrArray(types)).ids(ids))).version(true);
        return new SearchRequest(indices).indicesOptions(lenientExpandOpen()).source(source);
    }

    public static SearchRequest aggregationToRequest(AbstractAggregationBuilder aggs, String... indices) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).query(matchAllQuery()).aggregation(aggs);
        return new SearchRequest(indices).indicesOptions(lenientExpandOpen()).source(source);
    }

    public static SearchScrollRequest scrollRequest(StandardScroll scroll) {
        return new SearchScrollRequest(scroll.scrollId).scroll(timeValueMillis(scroll.scrollTimeInMillis));
    }

    public static SearchRequest queryToRequest(StandardQuery query, FilterBuilder postFilter) {
        SearchRequest request = new SearchRequest(toStrArray(query.indices)).types(toStrArray(query.types));
        SearchSourceBuilder builder = new SearchSourceBuilder().version(true);
        setFields(builder, Iterables.concat(query.fields, singleton(PARENT_META)));
        setHints(request, builder, query.queryHints);
        setFilters(builder, query);
        setAggregations(builder, query);
        setOrder(builder, query);
        setScrollOrLimits(request, builder, query);
        builder.postFilter(postFilter);
        request.source(builder);
        return request;
    }

    @VisibleForTesting
    static void setFields(SearchSourceBuilder builder, Iterable<String> fields) {
        builder.fields(ImmutableList.copyOf(fields));
        builder.fetchSource(true);
    }

    @VisibleForTesting
    static void setHints(SearchRequest request, SearchSourceBuilder builder, QueryHints hints) {
        request.indicesOptions(toOptions(hints.searchOptions));
        request.searchType(hints.searchType.toType());
        request.preference(hints.preference);
        String[] routings = toStrArray(hints.routing);
        if (routings.length > 0) request.routing(routings);
        if (!hints.fetchSource) builder.fetchSource(false);
        builder.timeout(timeValueSeconds(hints.timeoutInSeconds));
    }

    @VisibleForTesting
    static void setFilters(SearchSourceBuilder builder, StandardQuery query) {
        FilterBuilder[] filters = ExprConverter.aggregateFilters(query.expressions);
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
            builder.aggregation(AggsConverter.build(aggr));
    }

    @VisibleForTesting
    static void setOrder(SearchSourceBuilder builder, StandardQuery query) {
        for (Sort sort : query.sorts)
            builder.sort(fieldSort(sort.field).order(orderOf(sort.order)).sortMode(sort.mode.toMode())
               .unmappedType(sort.unmapped).missing(sort.missing));
    }

    private static void setScrollOrLimits(SearchRequest request, SearchSourceBuilder builder, StandardQuery query) {
        builder.size(query.size);
        if (query.scrollTimeInMillis != null) {
            request.scroll(timeValueMillis(query.scrollTimeInMillis));
        } else {
            builder.from(query.offset);
        }
    }

    public static String[] toStrArray(Iterable<String> iterable) {
        return toArray(iterable, String.class);
    }
}
