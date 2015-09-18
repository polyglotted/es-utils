package io.polyglotted.esutils.query.response;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.esutils.indexing.IndexKey;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.transform;

public interface ResultBuilder<T> {

    List<T> buildFrom(SearchResponse response);

    ResultBuilder<?> EmptyBuilder = response -> ImmutableList.of();

    ResultBuilder<SimpleDoc> SimpleDocBuilder = response -> ImmutableList.copyOf(transform(response.getHits(), hit -> {
        ImmutableMap<String, Object> source = (hit.getSource() == null) ? ImmutableMap.of() : copyOf(hit.getSource());
        return new SimpleDoc(IndexKey.from(hit), source);
    }));
}
