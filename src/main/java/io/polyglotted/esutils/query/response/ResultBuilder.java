package io.polyglotted.esutils.query.response;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.transform;

public interface ResultBuilder<T> {

    List<T> buildFrom(SearchResponse response);

    ResultBuilder<?> EmptyBuilder = response -> ImmutableList.of();

    ResultBuilder<SimpleDoc> SimpleDocBuilder = response -> ImmutableList.copyOf(transform(response.getHits(), hit -> {
        Map<String, Object> source = (hit.getSource() == null) ? ImmutableMap.of() : copyOf(hit.getSource());
        return new SimpleDoc(hit.index(), hit.type(), hit.id(), source);
    }));
}
