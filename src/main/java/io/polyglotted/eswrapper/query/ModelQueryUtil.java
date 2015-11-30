package io.polyglotted.eswrapper.query;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.esmodel.api.IndexKey;
import io.polyglotted.esmodel.api.query.SearchOptions;
import lombok.SneakyThrows;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Map;

import static io.polyglotted.esmodel.api.index.FieldMapping.STATUS_FIELD;
import static io.polyglotted.esmodel.api.query.SortOrder.ASC;
import static io.polyglotted.esmodel.api.query.SortOrder.DESC;
import static io.polyglotted.eswrapper.ElasticConstants.PARENT_META;
import static io.polyglotted.eswrapper.indexing.IndexRecord.Action.DELETE;
import static org.elasticsearch.action.support.IndicesOptions.readIndicesOptions;

abstract class ModelQueryUtil {
    private static final Map<SearchOptions, IndicesOptions> INDICES_OPTIONS_MAP = buildOptionsMap();

    public static SortOrder orderOf(io.polyglotted.esmodel.api.query.SortOrder order) {
        return (order == ASC) ? SortOrder.ASC : (order == DESC ? SortOrder.DESC : null);
    }

    public static IndicesOptions toOptions(SearchOptions options) {
        return INDICES_OPTIONS_MAP.get(options);
    }

    public static IndexKey keyFrom(SearchHit searchHit) {
        return new IndexKey(searchHit.getIndex(), searchHit.getType(), searchHit.getId(),
           searchHit.getVersion(), isDeleted(searchHit), getParent(searchHit));
    }

    private static boolean isDeleted(SearchHit searchHit) {
        Map<String, Object> source = searchHit.isSourceEmpty() ? ImmutableMap.of() : searchHit.sourceAsMap();
        return DELETE.status.equals(source.get(STATUS_FIELD));
    }

    private static String getParent(SearchHit searchHit) {
        SearchHitField field = searchHit.field(PARENT_META);
        return field == null ? null : (String) field.value();
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
