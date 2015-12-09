package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.polyglotted.eswrapper.services.IndexerException;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.getFirst;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Indexable {
    public final String unaryIndex;
    public final ImmutableList<IndexRecord> records;
    public final long timestamp;
    public final String user;

    public Collection<String> updateIds() {
        return transform(filter(records, IndexRecord::isUpdate), IndexRecord::id);
    }

    public Collection<IndexKey> updateKeys() {
        return transform(filter(records, IndexRecord::isUpdate), IndexRecord::key);
    }

    public BulkRequest updateRequest(Map<IndexKey, SimpleDoc> currentDocs) {
        BulkRequest request = new BulkRequest().refresh(false);
        validateCurrentDocs(currentDocs);
        for (IndexRecord record : records) {
            if (!record.isUpdate()) continue;

            log.debug("creating archive record " + record.uniqueId() + " for " + record.id()
               + " for type " + record.type() + " at " + record.index());

            request.add(new IndexRequest(record.index(), record.type(), record.uniqueId()).create(true)
               .parent(record.parent()).versionType(VersionType.EXTERNAL).version(record.version())
               .source(record.action.sourceFrom(currentDocs.get(record.indexKey), timestamp, user)));
        }
        return request;
    }

    public BulkRequest writeRequest() {
        return new BulkRequest().refresh(false).add(transform(records, record -> record.request(timestamp, user)));
    }

    private void validateCurrentDocs(Map<IndexKey, SimpleDoc> currentDocs) {
        ImmutableMap.Builder<IndexKey, String> builder = ImmutableMap.builder();
        int count = 0;
        for (IndexKey indexKey : updateKeys()) {
            if (!currentDocs.containsKey(indexKey)) {
                count++;
                builder.put(indexKey, "record not found for update");
            }
        }
        if (count > 0) throw new IndexerException(builder.build());
    }

    public static Builder indexableBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final Set<IndexRecord> records = new LinkedHashSet<>();
        private long timestamp = System.currentTimeMillis();
        private String user = "unknown";

        public Builder records(Iterable<IndexRecord> records) {
            for (IndexRecord record : records)
                checkArgument(this.records.add(record), "record already exists {}", record.indexKey);
            return this;
        }

        public Indexable build() {
            ImmutableSet<String> indices = ImmutableSet.copyOf(transform(records, IndexRecord::index));
            checkArgument(indices.size() == 1, "cannot create indexable on multiple indices");
            return new Indexable(getFirst(indices, "none"), copyOf(records), timestamp, user);
        }
    }
}
