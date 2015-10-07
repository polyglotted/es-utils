package io.polyglotted.eswrapper.query.response;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.search.SearchResponse;

import java.util.Objects;

@RequiredArgsConstructor
@ToString(doNotUseGetters = true)
public final class ResponseHeader {
    public final long millis;
    public final long hits;
    public final long returned;
    public final String id;

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
        return (hits == that.hits) && (returned == that.returned) &&
           (id == null ? that.id == null : id.equals(that.id));
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits, returned, id);
    }
}
