package io.polyglotted.eswrapper.query.response;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.polyglotted.eswrapper.indexing.IndexKey;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.transform;

public interface ResultBuilder<T> {

    List<T> buildFrom(SearchResponse response);

    ResultBuilder<?> NullBuilder = response -> ImmutableList.of();

    ResultBuilder<IndexKey> IndexKeyBuilder = response -> ImmutableList.copyOf(transform(response.getHits(),
       hit -> IndexKey.from(hit)));

    ResultBuilder<SimpleDoc> SimpleDocBuilder = response -> ImmutableList.copyOf(transform(response.getHits(), hit -> {
        ImmutableMap<String, Object> source = (hit.getSource() == null) ? ImmutableMap.of() : copyOf(hit.getSource());
        return new SimpleDoc(IndexKey.from(hit), source);
    }));

    static <T> ResultBuilder<T> SimpleObjectBuilder(final Gson gson, Class<T> clazz) {
        return response -> ImmutableList.copyOf(transform(response.getHits(),
           hit -> hit.isSourceEmpty() ? null : gson.fromJson(hit.sourceAsString(), clazz)));
    }
}
