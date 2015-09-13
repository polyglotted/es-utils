package io.polyglotted.esutils.query.response;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;

@RequiredArgsConstructor
public final class ResponseHeader {
    public final long tookInMillis;
    public final long totalHits;
    public final long returnedHits;
    public final String scrollId;

    public static ResponseHeader headerFrom(SearchResponse response) {
        return new ResponseHeader(response.getTookInMillis(), getTotalHits(response),
           getReturnedHits(response), response.getScrollId());
    }

    public static long getTotalHits(SearchResponse response) {
        SearchHits hits = response.getHits();
        return hits == null ? 0 : hits.getTotalHits();
    }

    public static int getReturnedHits(SearchResponse response) {
        SearchHits hits = response.getHits();
        return hits == null ? 0 : hits.getHits().length;
    }
}
