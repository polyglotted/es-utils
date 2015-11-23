package io.polyglotted.eswrapper.query.response;

import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.search.SearchResponse;

import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
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
        return response.getHits().getTotalHits();
    }

    public static int getReturnedHits(SearchResponse response) {
        return response.getHits().hits().length;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) &&
           equalizer().equals(((ResponseHeader) o).equalizer()));
    }

    @Override
    public int hashCode() {
        return 31 * equalizer().hashCode();
    }

    private String equalizer() {
        return GSON.toJson(ImmutableList.of(totalHits, returnedHits, scrollId == null ? "" : scrollId));
    }
}
