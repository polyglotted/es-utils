package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.esmodel.api.IndexKey;
import io.polyglotted.esmodel.api.SimpleDoc;
import io.polyglotted.esmodel.api.query.*;
import io.polyglotted.eswrapper.query.AggsConverter;
import io.polyglotted.eswrapper.query.ResultBuilder;
import io.polyglotted.eswrapper.query.SourceBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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

import static com.google.common.base.Preconditions.checkArgument;
import static io.polyglotted.eswrapper.query.QueryBuilder.aggregationToRequest;
import static io.polyglotted.eswrapper.query.QueryBuilder.queryToRequest;
import static io.polyglotted.eswrapper.query.QueryBuilder.scrollRequest;
import static io.polyglotted.eswrapper.services.ModelUtil.getReturnedHits;
import static io.polyglotted.eswrapper.services.ModelUtil.getTotalHits;
import static io.polyglotted.eswrapper.services.ModelUtil.headerFrom;
import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@Slf4j
@RequiredArgsConstructor
public final class QueryWrapper {

    private static final TimeValue DEFAULT_KEEP_ALIVE = timeValueMinutes(30);
    private final Client client;

    public Map<String, String> indexStatus(String index) {
        GetSettingsRequestBuilder settingsRequest = client.admin()
           .indices().prepareGetSettings(index).setIndicesOptions(lenientExpandOpen());
        GetSettingsResponse response = settingsRequest.execute().actionGet();

        ImmutableOpenMap<String, Settings> indexToSettings = response.getIndexToSettings();
        Iterator<String> it = indexToSettings.keysIt();
        return (it.hasNext()) ? ImmutableMap.copyOf(indexToSettings.get(it.next()).getAsMap())
           : ImmutableMap.of();
    }

    public SimpleDoc get(IndexKey indexKey) {
        GetResponse response = client.get(new GetRequest(indexKey.index, indexKey.type, indexKey.id)).actionGet();
        checkArgument(response.isExists(), "unable to find document with id " + indexKey.id);

        return new SimpleDoc(indexKey.version(response.getVersion()), ImmutableMap.copyOf(response.getSourceAsMap()));
    }

    public <T> T getAs(IndexKey indexKey, SourceBuilder<T> builder) {
        GetResponse response = client.get(new GetRequest(indexKey.index, indexKey.type, indexKey.id)).actionGet();
        checkArgument(response.isExists(), "unable to find document with id " + indexKey.id);

        return builder.buildFrom(response.getSourceAsMap());
    }

    public Aggregation aggregate(Expression aggs, String... indices) {
        SearchResponse response = client.search(aggregationToRequest(AggsConverter.build(aggs), indices)).actionGet();
        return buildAggregations(singletonList(aggs), response).get(0);
    }

    public <T> QueryResponse simpleScroll(SearchRequest request, ResultBuilder<T> resultBuilder) {
        request = request.scroll(DEFAULT_KEEP_ALIVE);
        SearchResponse searchResponse = client.search(request).actionGet();

        QueryResponse.Builder response = QueryResponse.responseBuilder();
        final long totalHits = getTotalHits(searchResponse);
        long tookInMillis = 0;
        while (getReturnedHits(searchResponse) > 0) {
            tookInMillis += searchResponse.getTookInMillis();
            response.results(resultBuilder.buildFrom(searchResponse));

            SearchScrollRequest scrollRequest = new SearchScrollRequest(searchResponse.getScrollId())
               .scroll(DEFAULT_KEEP_ALIVE);
            searchResponse = client.searchScroll(scrollRequest).actionGet();
        }
        response.header(new ResponseHeader(tookInMillis, totalHits, totalHits, null));
        return response.build();
    }

    public <T> QueryResponse scroll(StandardScroll scroll, ResultBuilder<T> resultBuilder) {
        SearchResponse searchResponse = client.searchScroll(scrollRequest(scroll)).actionGet();
        return responseBuilder(searchResponse, resultBuilder).build();
    }

    public <T> QueryResponse search(SearchRequest request, ResultBuilder<T> resultBuilder) {
        SearchResponse searchResponse = client.search(request).actionGet();
        return responseBuilder(searchResponse, resultBuilder).build();
    }

    public <T> QueryResponse search(StandardQuery query, FilterBuilder postFilter, ResultBuilder<T> resultBuilder) {
        SearchResponse searchResponse = client.search(queryToRequest(query, postFilter)).actionGet();
        return responseBuilder(searchResponse, resultBuilder)
           .aggregations(buildAggregations(query.aggregates, searchResponse)).build();
    }

    private static <T> QueryResponse.Builder responseBuilder(SearchResponse searchResponse, ResultBuilder<T>
       resultBuilder) {

        QueryResponse.Builder responseBuilder = QueryResponse.responseBuilder();
        responseBuilder.header(headerFrom(searchResponse));
        if (getReturnedHits(searchResponse) > 0) responseBuilder.results(resultBuilder.buildFrom(searchResponse));
        return responseBuilder;
    }

    private static List<Aggregation> buildAggregations(List<Expression> aggregates, SearchResponse response) {
        if (aggregates.isEmpty()) return ImmutableList.of();

        Aggregations aggregations = response.getAggregations();
        ImmutableList.Builder<Aggregation> result = ImmutableList.builder();
        for (Expression expr : aggregates) {
            result.add(AggsConverter.get(expr, aggregations));
        }
        return result.build();
    }
}
