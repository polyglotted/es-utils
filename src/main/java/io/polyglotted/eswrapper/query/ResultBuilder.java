package io.polyglotted.eswrapper.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.ImmutableMap.of;
import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.query.ModelQueryUtil.keyFrom;

public interface ResultBuilder<T> {

    default List<T> buildFrom(SearchResponse response) {
        return ImmutableList.copyOf(transform(response.getHits(), hit -> {
            ImmutableMap<String, Object> source = hit.isSourceEmpty() ? of() : copyOf(hit.getSource());
            return buildResult(keyFrom(hit), source);
        }));
    }

    default T buildFrom(GetResponse response) {
        ImmutableMap<String, Object> source = response.isSourceEmpty() ? of() : copyOf(response.getSourceAsMap());
        return buildResult(keyFrom(response), source);
    }

    T buildResult(IndexKey key, ImmutableMap<String, Object> source);

    ResultBuilder<?> NullBuilder = (key, source) -> ImmutableMap.of();

    ResultBuilder<IndexKey> IndexKeyBuilder = (key, source) -> key;

    ResultBuilder<Map<String, ?>> SourceBuilder = (key, source) -> source;

    ResultBuilder<SimpleDoc> SimpleDocBuilder = SimpleDoc::new;

    static <T> ResultBuilder<T> SimpleObjectBuilder(final Gson gson, Class<T> clazz) {
        return (key, source) -> source.isEmpty() ? null : gson.fromJson(gson.toJson(source), clazz);
    }
}
