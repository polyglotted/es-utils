package io.polyglotted.eswrapper.query;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.query.SearchOptions;
import lombok.SneakyThrows;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Map;

import static io.polyglotted.eswrapper.ElasticConstants.PARENT_META;
import static io.polyglotted.pgmodel.search.query.SortOrder.ASC;
import static org.elasticsearch.action.support.IndicesOptions.readIndicesOptions;

public abstract class ModelQueryUtil {
    private static final Map<SearchOptions, IndicesOptions> INDICES_OPTIONS_MAP = buildOptionsMap();

    public static SortOrder orderOf(io.polyglotted.pgmodel.search.query.SortOrder order) {
        return (order == ASC) ? SortOrder.ASC : SortOrder.DESC;
    }

    public static IndicesOptions toOptions(SearchOptions options) {
        return INDICES_OPTIONS_MAP.get(options);
    }

    public static IndexKey keyFrom(SearchHit searchHit) {
        return IndexKey.keyFrom(searchHit.getIndex(), searchHit.getType(), searchHit.getId(),
           getParent(searchHit), searchHit.getVersion());
    }

    private static String getParent(SearchHit searchHit) {
        SearchHitField field = searchHit.field(PARENT_META);
        return field == null ? null : (String) field.value();
    }

    public static IndexKey keyFrom(GetResponse response) {
        return IndexKey.keyFrom(response.getIndex(), response.getType(), response.getId(),
           getParent(response), response.getVersion());
    }

    private static String getParent(GetResponse response) {
        GetField field = response.getField(PARENT_META);
        return field == null ? null : (String) field.getValue();
    }

    @SneakyThrows
    private static ImmutableMap<SearchOptions, IndicesOptions> buildOptionsMap() {
        ImmutableMap.Builder<SearchOptions, IndicesOptions> builder = ImmutableMap.builder();
        for (SearchOptions options : SearchOptions.values()) {
            builder.put(options, readIndicesOptions(new BytesStreamInput(options.bytes)));
        }
        return builder.build();
    }
}
