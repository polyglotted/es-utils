package io.polyglotted.esutils.query.response;

import org.elasticsearch.action.search.SearchResponse;

public interface ResultBuilder<T> {

    Iterable<T> buildFrom(SearchResponse response);
}
