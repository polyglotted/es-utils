package io.polyglotted.esutils.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

@Slf4j
@RequiredArgsConstructor
public final class IndexerWrapper {
    private final Client client;

    public void index(BulkRequest bulkRequest) {
        if (bulkRequest.numberOfActions() <= 0) return;

        BulkResponse responses = client.bulk(bulkRequest).actionGet();
        if (responses.hasFailures()) {
            throw new RuntimeException("elastic-search error " + responses.buildFailureMessage());
        }
    }
}
