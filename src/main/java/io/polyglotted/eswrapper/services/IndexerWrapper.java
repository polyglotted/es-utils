package io.polyglotted.eswrapper.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.indexing.IgnoreErrors;
import io.polyglotted.eswrapper.indexing.IndexKey;
import io.polyglotted.eswrapper.indexing.IndexerException;
import io.polyglotted.eswrapper.indexing.VersionIndexable;
import io.polyglotted.eswrapper.query.response.SimpleDoc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.IgnoreErrors.lenient;
import static io.polyglotted.eswrapper.indexing.IgnoreErrors.strict;
import static io.polyglotted.eswrapper.query.request.QueryBuilder.idRequest;
import static io.polyglotted.eswrapper.query.response.ResultBuilder.SimpleDocBuilder;
import static org.elasticsearch.client.Requests.refreshRequest;

@Slf4j
@RequiredArgsConstructor
public final class IndexerWrapper {
    private final Client client;

    public void index(BulkRequest bulkRequest) {
        index(bulkRequest, strict());
    }

    public void twoPhaseCommit(VersionIndexable indexable) {
        lockTheIndexOrFail(indexable.currentIndex);
        List<SimpleDoc> currentDocs = getCurrent(indexable.currentIndex, indexable.currentIds());
        try {
            index(indexable.updateRequest());
            index(indexable.writeRequest());

        } catch (IndexerException ex) {
            log.error("failed two phase commit", ex);
            deleteUpdatesInHistory(indexable.historyIndex, indexable.updateKeys());
            forceReindex(currentDocs);
            throw ex;

        } finally {
            unlockIndex(indexable.currentIndex);
            client.admin().indices().refresh(refreshRequest(indexable.indices())).actionGet();
        }
    }

    @VisibleForTesting
    List<SimpleDoc> getCurrent(String currentIndex, String... ids) {
        return SimpleDocBuilder.buildFrom(client.search(idRequest(ids, ImmutableList.of(), currentIndex)).actionGet());
    }

    @VisibleForTesting
    void forceReindex(List<SimpleDoc> currentDocs) {
        index(new BulkRequest().refresh(false).add(transform(currentDocs, SimpleDoc::forcedRequest)), lenient());
    }

    @VisibleForTesting
    void deleteUpdatesInHistory(String historyIndex, Iterable<IndexKey> items) {
        index(new BulkRequest().refresh(false).add(transform(items,
           indexKey -> new DeleteRequest(historyIndex, indexKey.type, indexKey.id))), lenient());
    }

    @VisibleForTesting
    void lockTheIndexOrFail(String index) {
        try {
            IndexResponse response = client.index(new IndexRequest(index, "$lock", "global")
               .create(true).source("_val", true)).actionGet();
            checkState(response.isCreated(), "unable to lock the index " + index);
        } catch (DocumentAlreadyExistsException dex) {
            throw new IllegalStateException("unable to lock the index " + index);
        }
    }

    @VisibleForTesting
    void unlockIndex(String index) {
        client.delete(new DeleteRequest(index, "$lock", "global")).actionGet();
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
            if (!ignore.ignoreFailure(failureMessage)) {
                errorBuilder.put(IndexKey.from(response), failureMessage);
            }
        }
        ImmutableMap<IndexKey, String> errors = errorBuilder.build();
        if (errors.size() > 0) throw new IndexerException(errors);
    }
}
