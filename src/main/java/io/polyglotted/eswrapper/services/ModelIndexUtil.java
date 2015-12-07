package io.polyglotted.eswrapper.services;

import io.polyglotted.esmodel.api.IndexKey;
import io.polyglotted.esmodel.api.SimpleDoc;
import io.polyglotted.esmodel.api.index.Alias;
import io.polyglotted.esmodel.api.query.ResponseHeader;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.index.VersionType;

import static com.google.common.collect.Iterables.toArray;
import static io.polyglotted.eswrapper.query.ExprConverter.buildFilter;

abstract class ModelIndexUtil {

    public static IndicesAliasesRequest.AliasActions aliasActions(Alias alias) {
        return new IndicesAliasesRequest.AliasActions(alias.remove ? AliasAction.Type.REMOVE : AliasAction.Type.ADD,
           toArray(alias.indices, String.class), new String[]{alias.alias}).filter(buildFilter(alias.filter));
    }

    public static ActionRequest forcedRequest(SimpleDoc doc) {
        return new IndexRequest(doc.key.index, doc.key.type, doc.key.id).version(doc.key.version)
           .versionType(VersionType.FORCE).source(doc.source);
    }

    public static IndexKey keyFrom(IndexResponse response) {
        return new IndexKey(response.getIndex(), response.getType(), response.getId(), response.getVersion());
    }

    public static IndexKey keyFrom(BulkItemResponse response) {
        return new IndexKey(response.getIndex(), response.getType(), response.getId(), response.getVersion());
    }

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
}
