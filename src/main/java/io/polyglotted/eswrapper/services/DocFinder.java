package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import io.polyglotted.eswrapper.query.ResultBuilder;
import io.polyglotted.pgmodel.search.IndexKey;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;

import java.util.List;

import static io.polyglotted.eswrapper.ElasticConstants.PARENT_META;
import static io.polyglotted.eswrapper.ElasticConstants.SOURCE_META;
import static io.polyglotted.eswrapper.services.ModelIndexUtil.checkMultiGet;

public abstract class DocFinder {

    public static <T> T findBy(Client client, String index, String type, String id, String
       parent, ResultBuilder<T> builder) {
        GetResponse response = client.get(new GetRequest(index, type, id).parent(parent)
           .fields(SOURCE_META, PARENT_META)).actionGet();
        return response.isExists() ? builder.buildFrom(response) : null;
    }

    public static <T> List<T> findAllBy(Client client, Iterable<IndexKey> indexKeys, ResultBuilder<T>
       resultBuilder, boolean ignoreFailure) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (IndexKey key : indexKeys) {
            multiGetRequest.add(new MultiGetRequest.Item(key.index, key.type, key.id).parent(key.parent)
               .fields(SOURCE_META, PARENT_META));
        }
        MultiGetResponse multiGetItemResponses = client.multiGet(multiGetRequest).actionGet();

        ImmutableList.Builder<T> result = ImmutableList.builder();
        for (MultiGetItemResponse item : multiGetItemResponses.getResponses()) {
            GetResponse get = checkMultiGet(item).getResponse();
            if (!get.isExists()) {
                if (ignoreFailure) continue;
                throw new IllegalStateException("get item not exists");
            }
            result.add(resultBuilder.buildFrom(get));
        }
        return result.build();
    }
}
