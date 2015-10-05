package io.polyglotted.eswrapper.query.response;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.search.SearchResponse;

import java.util.Objects;

@RequiredArgsConstructor
@ToString(doNotUseGetters = true)
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResponseHeader that = (ResponseHeader) o;
        return (totalHits == that.totalHits) && (returnedHits == that.returnedHits) &&
           (scrollId == null ? that.scrollId == null : scrollId.equals(that.scrollId));
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalHits, returnedHits, scrollId);
    }
}
