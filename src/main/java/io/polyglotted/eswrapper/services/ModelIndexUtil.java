package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import io.polyglotted.pgmodel.search.index.Alias;
import io.polyglotted.pgmodel.search.query.ResponseHeader;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.index.VersionType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.toArray;
import static io.polyglotted.eswrapper.query.ExprConverter.buildFilter;
import static io.polyglotted.pgmodel.search.IndexKey.keyFrom;

public abstract class ModelIndexUtil {

    public static IndicesAliasesRequest.AliasActions aliasActions(Alias alias) {
        return new IndicesAliasesRequest.AliasActions(alias.remove ? AliasAction.Type.REMOVE : AliasAction.Type.ADD,
           toArray(alias.indices, String.class), new String[]{alias.alias}).filter(buildFilter(alias.filter));
    }

    public static ActionRequest forcedRequest(SimpleDoc doc) {
        return new IndexRequest(doc.key.index, doc.key.type, doc.key.id).version(doc.key.version)
           .versionType(VersionType.FORCE).source(doc.source);
    }

    public static List<IndexKey> resultKeys(BulkResponse response, List<IndexKey> currentKeys) {
        BulkItemResponse[] responseItems = response.getItems();
        checkArgument(responseItems.length == currentKeys.size(), "size mismatch of keys and responses");

        ImmutableList.Builder<IndexKey> keys = ImmutableList.builder();
        for (int i = 0; i < currentKeys.size(); i++) keys.add(indexKeyOf(responseItems[i], currentKeys.get(i).parent));
        return keys.build();
    }

    public static IndexKey indexKeyOf(BulkItemResponse response, String parent) {
        return keyFrom(response.getIndex(), response.getType(), response.getId(), parent, response.getVersion());
    }

    public static ResponseHeader headerFrom(SearchResponse response) {
        return new ResponseHeader(response.getTookInMillis(), getTotalHits(response),
           getReturnedHits(response), response.getScrollId());
    }

    public static long getTotalHits(SearchResponse response) { return response.getHits().getTotalHits(); }

    public static int getReturnedHits(SearchResponse response) { return response.getHits().hits().length; }

    public static MultiGetItemResponse checkMultiGet(MultiGetItemResponse item) {
        if (item.isFailed())
            throw new IllegalStateException("error multi-get item " + failureMessage(item.getFailure()));
        return item;
    }

    private static String failureMessage(MultiGetResponse.Failure fail) {
        return fail.getIndex() + "/" + fail.getType() + "/" + fail.getId() + ": " + fail.getMessage();
    }
}
