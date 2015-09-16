package io.polyglotted.esutils.services;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.esutils.indexing.IgnoreErrors;
import io.polyglotted.esutils.indexing.IndexKey;
import io.polyglotted.esutils.indexing.IndexerException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

@Slf4j
@RequiredArgsConstructor
public final class IndexerWrapper {
    private final Client client;

    public void index(BulkRequest bulkRequest) {
        index(bulkRequest, IgnoreErrors.strict());
    }

    public void index(BulkRequest bulkRequest, IgnoreErrors ignoreErrors) {
        if (bulkRequest.numberOfActions() <= 0) return;
        BulkResponse responses = client.bulk(bulkRequest).actionGet();
        checkResponse(responses, ignoreErrors);
    }

    private static void checkResponse(BulkResponse responses, IgnoreErrors ignore) {
        if (!responses.hasFailures()) return;

        ImmutableMap.Builder<IndexKey, String> errorBuilder = ImmutableMap.builder();
        for (BulkItemResponse response : responses) {
            String failureMessage = response.getFailureMessage();
            if (response.isFailed() && !ignore.ignoreFailure(failureMessage)) {
                errorBuilder.put(IndexKey.from(response), failureMessage);
            }
        }
        ImmutableMap<IndexKey, String> errors = errorBuilder.build();
        if (errors.size() > 0) throw new IndexerException(errors);
    }

}
