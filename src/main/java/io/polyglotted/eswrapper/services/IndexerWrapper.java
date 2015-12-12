package io.polyglotted.eswrapper.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.indexing.Bundling;
import io.polyglotted.eswrapper.indexing.IgnoreErrors;
import io.polyglotted.eswrapper.indexing.Indexable;
import io.polyglotted.eswrapper.validation.Validator;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.polyglotted.eswrapper.indexing.IgnoreErrors.lenient;
import static io.polyglotted.eswrapper.indexing.IgnoreErrors.strict;
import static io.polyglotted.eswrapper.query.QueryBuilder.idRequest;
import static io.polyglotted.eswrapper.query.ResultBuilder.SimpleDocBuilder;
import static io.polyglotted.eswrapper.services.ModelIndexUtil.keyFrom;
import static io.polyglotted.eswrapper.validation.ValidException.checkValidity;
import static io.polyglotted.eswrapper.validation.Validator.EMPTY_VALIDATOR;
import static org.elasticsearch.client.Requests.refreshRequest;

@Slf4j
@RequiredArgsConstructor
public final class IndexerWrapper {
    private final Client client;

    public void index(ActionRequest<?> request) {
        if (request instanceof IndexRequest)
            client.index((IndexRequest) request).actionGet();
        else if (request instanceof UpdateRequest)
            client.update((UpdateRequest) request).actionGet();
        else if (request instanceof DeleteRequest)
            client.delete((DeleteRequest) request).actionGet();
    }

    public BulkResponse index(BulkRequest bulkRequest) {
        return index(bulkRequest, strict());
    }

    public List<IndexKey> bulkIndex(Bundling bundling, IgnoreErrors ignoreErrors) {
        try {
            BulkResponse bulkResponse = index(bundling.writeRequest(), ignoreErrors);
            return ImmutableList.copyOf(transform(checkNotNull(bulkResponse), ModelIndexUtil::keyFrom));

        } finally {
            forceRefresh(bundling.indices());
        }
    }

    public List<IndexKey> twoPhaseCommit(Indexable indexable) {
        return twoPhaseCommit(indexable, EMPTY_VALIDATOR);
    }

    public List<IndexKey> twoPhaseCommit(Indexable indexable, Validator validator) {
        lockTheIndexOrFail(indexable.unaryIndex);
        try {
            List<SimpleDoc> currentDocs = validateAndGet(indexable, validator);
            BulkRequest updateRequest = indexable.updateRequest(uniqueIndex(currentDocs, SimpleDoc::key));
            try {
                index(updateRequest);
                BulkResponse bulkResponse = index(indexable.writeRequest());
                return ImmutableList.copyOf(transform(bulkResponse, ModelIndexUtil::keyFrom));

            } catch (RuntimeException ex) {
                log.error("failed two phase commit", ex);
                deleteUpdatesInHistory(indexable.unaryIndex, indexable.updateKeys());
                forceReindex(currentDocs);
                throw ex;
            }
        } finally {
            unlockIndex(indexable.unaryIndex);
            forceRefresh(indexable.unaryIndex);
        }
    }

    private List<SimpleDoc> validateAndGet(Indexable indexable, Validator validator) {
        Collection<String> updateIds = indexable.updateIds();
        List<SimpleDoc> currentDocs = getCurrent(indexable.unaryIndex, toArray(updateIds, String.class));
        checkValidity(validator.validate(updateIds));
        return currentDocs;
    }

    private void forceRefresh(String... indices) {
        client.admin().indices().refresh(refreshRequest(indices)).actionGet();
    }

    @VisibleForTesting
    List<SimpleDoc> getCurrent(String currentIndex, String... ids) {
        return SimpleDocBuilder.buildFrom(client.search(idRequest(ids, ImmutableList.of(), currentIndex)).actionGet());
    }

    @VisibleForTesting
    void forceReindex(List<SimpleDoc> currentDocs) {
        index(new BulkRequest().refresh(false).add(transform(currentDocs, ModelIndexUtil::forcedRequest)), lenient());
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
               .create(true).source(ImmutableMap.of())).actionGet();
            checkState(response.isCreated(), "unable to lock the index " + index);
        } catch (DocumentAlreadyExistsException dex) {
            throw new IllegalStateException("unable to lock the index " + index);
        }
    }

    @VisibleForTesting
    void unlockIndex(String index) {
        client.delete(new DeleteRequest(index, "$lock", "global")).actionGet();
    }

    public BulkResponse index(BulkRequest bulkRequest, IgnoreErrors ignoreErrors) {
        if (bulkRequest.numberOfActions() <= 0) return null;
        BulkResponse responses = client.bulk(bulkRequest).actionGet();
        checkResponse(responses, ignoreErrors);
        return responses;
    }

    private static void checkResponse(BulkResponse responses, IgnoreErrors ignore) {
        if (!responses.hasFailures()) return;

        ImmutableMap.Builder<IndexKey, String> errorBuilder = ImmutableMap.builder();
        for (BulkItemResponse response : responses) {
            String failureMessage = response.getFailureMessage();
            if (!ignore.ignoreFailure(failureMessage)) {
                errorBuilder.put(keyFrom(response), failureMessage);
            }
        }
        ImmutableMap<IndexKey, String> errors = errorBuilder.build();
        if (errors.size() > 0) throw new IndexerException(errors);
    }

    public long generateSequence(IndexKey key) {
        IndexResponse indexResponse = client.index(new IndexRequest(key.index, key.type, key.id)
           .source(ImmutableMap.of())).actionGet();
        return indexResponse.getVersion();
    }

    public List<Long> generateSequences(IndexKey key, int blocks) {
        BulkRequest request = new BulkRequest().refresh(true);
        for (int i = 0; i < blocks; i++)
            request.add(new IndexRequest(key.index, key.type, key.id).source(ImmutableMap.of()));

        return ImmutableList.copyOf(transform(index(request), BulkItemResponse::getVersion));
    }
}
