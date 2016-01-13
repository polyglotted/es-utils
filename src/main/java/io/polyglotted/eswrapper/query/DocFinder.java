package io.polyglotted.eswrapper.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.uniqueIndex;
import static io.polyglotted.eswrapper.services.ModelIndexUtil.checkMultiGet;

public abstract class DocFinder {

    public static SimpleDoc getByKey(Client client, IndexKey indexKey) {
        GetResponse response = client.get(new GetRequest(indexKey.index, indexKey.type, indexKey.id)).actionGet();
        return response.isExists() ? new SimpleDoc(indexKey.newVersion(response.getVersion()),
           ImmutableMap.copyOf(response.getSourceAsMap())) : null;
    }

    public static <T> T getByKeyAs(Client client, IndexKey indexKey, SourceBuilder<T> builder) {
        GetResponse response = client.get(new GetRequest(indexKey.index, indexKey.type, indexKey.id)).actionGet();
        return response.isExists() ? builder.buildFrom(response.getSourceAsMap()) : null;
    }

    public static Map<IndexKey, SimpleDoc> findAllByKeys(Client client, Iterable<IndexKey> indexKeys) {
        return uniqueIndex(multiGetByKeys(client, indexKeys, true), SimpleDoc::key);
    }

    public static List<SimpleDoc> multiGetByKeys(Client client, Iterable<IndexKey> indexKeys, boolean ignoreFailure) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (IndexKey key : indexKeys) multiGetRequest.add(new MultiGetRequest.Item(key.index, key.type, key.id));
        MultiGetResponse multiGetItemResponses = client.multiGet(multiGetRequest).actionGet();
        ImmutableList.Builder<SimpleDoc> result = ImmutableList.builder();

        for (MultiGetItemResponse item : multiGetItemResponses.getResponses()) {
            GetResponse get = checkMultiGet(item).getResponse();
            if (!get.isExists()) {
                if (ignoreFailure) continue;
                throw new IllegalStateException("get item not exists");
            }
            result.add(new SimpleDoc(IndexKey.keyFrom(get.getIndex(), get.getType(), get.getId(), get.getVersion()),
               ImmutableMap.copyOf(get.getSourceAsMap())));
        }
        return result.build();
    }
}
