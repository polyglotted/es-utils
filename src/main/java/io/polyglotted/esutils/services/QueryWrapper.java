package io.polyglotted.esutils.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.esutils.query.AggregationType;
import io.polyglotted.esutils.query.StandardQuery;
import io.polyglotted.esutils.query.StandardResponse;
import io.polyglotted.esutils.query.request.Expression;
import io.polyglotted.esutils.query.response.Aggregation;
import io.polyglotted.esutils.query.response.ResultBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.Aggregations;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.polyglotted.esutils.query.request.QueryBuilder.aggregationToRequest;
import static io.polyglotted.esutils.query.request.QueryBuilder.queryToRequest;
import static io.polyglotted.esutils.query.response.ResponseHeader.getReturnedHits;
import static io.polyglotted.esutils.query.response.ResponseHeader.headerFrom;
import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@Slf4j
@RequiredArgsConstructor
public final class QueryWrapper {

    private static final TimeValue DEFAULT_KEEP_ALIVE = timeValueMinutes(30);
    private final Client client;

    public Map<String, Map<String, String>> indexStatus(String... indices) {
        ImmutableMap.Builder<String, Map<String, String>> result = ImmutableMap.builder();

        GetSettingsRequestBuilder settingsRequest = client.admin()
           .indices().prepareGetSettings(indices).setIndicesOptions(lenientExpandOpen());
        GetSettingsResponse response = settingsRequest.execute().actionGet();

        ImmutableOpenMap<String, Settings> indexToSettings = response.getIndexToSettings();
        for (Iterator<String> it = indexToSettings.keysIt(); it.hasNext(); ) {
            String indexName = it.next();
            result.put(indexName, ImmutableMap.copyOf(indexToSettings.get(indexName).getAsMap()));
        }

        return result.build();
    }

    public Aggregation aggregate(Expression aggs, String... indices) {
        SearchResponse response = client.search(aggregationToRequest(AggregationType.build(aggs), indices)).actionGet();
        return buildAggregations(singletonList(aggs), response).get(0);
    }

    @SuppressWarnings("unused")
    public <T> StandardResponse scroll(SearchRequest request, ResultBuilder<T> resultBuilder) {
        request = request.scroll(DEFAULT_KEEP_ALIVE);
        SearchResponse searchResponse = client.search(request).actionGet();

        StandardResponse.Builder response = StandardResponse.builder();
        response.header(headerFrom(searchResponse));

        while (getReturnedHits(searchResponse) > 0) {
            response.results(resultBuilder.buildFrom(searchResponse));

            SearchScrollRequest scrollRequest = new SearchScrollRequest(
               searchResponse.getScrollId()).scroll(DEFAULT_KEEP_ALIVE);
            searchResponse = client.searchScroll(scrollRequest).actionGet();
        }
        return response.build();
    }

    public <T> StandardResponse scroll(SearchScrollRequest request, ResultBuilder<T> resultBuilder) {
        SearchResponse searchResponse = client.searchScroll(request).actionGet();
        return responseBuilder(searchResponse, resultBuilder).build();
    }

    public <T> StandardResponse search(SearchRequest request, ResultBuilder<T> resultBuilder) {
        SearchResponse searchResponse = client.search(request).actionGet();
        return responseBuilder(searchResponse, resultBuilder).build();
    }

    public <T> StandardResponse search(StandardQuery query, FilterBuilder postFilter, ResultBuilder<T> resultBuilder) {

        SearchResponse searchResponse = client.search(queryToRequest(query, postFilter)).actionGet();
        return responseBuilder(searchResponse, resultBuilder)
           .aggregations(buildAggregations(query.aggregates, searchResponse)).build();
    }

    private static <T> StandardResponse.Builder responseBuilder(SearchResponse searchResponse, ResultBuilder<T>
       resultBuilder) {

        StandardResponse.Builder responseBuilder = StandardResponse.builder();
        responseBuilder.header(headerFrom(searchResponse));
        responseBuilder.results(resultBuilder.buildFrom(searchResponse));
        return responseBuilder;
    }

    private static List<Aggregation> buildAggregations(List<Expression> aggregates, SearchResponse response) {
        if (aggregates.isEmpty()) return ImmutableList.of();

        Aggregations aggregations = response.getAggregations();
        ImmutableList.Builder<Aggregation> result = ImmutableList.builder();
        for (Expression expr : aggregates) {
            result.add(AggregationType.get(expr, aggregations));
        }
        return result.build();
    }
}
