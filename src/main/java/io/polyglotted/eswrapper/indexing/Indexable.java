package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.getFirst;
import static io.polyglotted.eswrapper.services.ValidityException.checkValidity;
import static io.polyglotted.pgmodel.search.KeyUtil.longToCompare;
import static java.util.Arrays.asList;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Indexable {
    public final String unaryIndex;
    public final ImmutableList<IndexRecord> records;
    public final long timestamp;
    public final String user;

    public Collection<String> updateIds() {
        return transform(records, IndexRecord::id);
    }

    public Collection<IndexKey> updateKeys() {
        return transform(records, IndexRecord::key);
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
               .source(record.action.sourceFrom(currentDocs.get(record.indexKey),
                  record.updateStatus, record.updateComment, timestamp, user)));
        }
        return request;
    }

    public BulkRequest writeRequest() {
        return new BulkRequest().refresh(false).add(transform(records, record -> record.request(timestamp, user)));
    }

    private void validateCurrentDocs(Map<IndexKey, SimpleDoc> currentDocs) {
        ImmutableMap.Builder<IndexKey, String> builder = ImmutableMap.builder();
        for (IndexRecord record : records) {
            IndexKey indexKey = record.indexKey;

            if (record.isUpdate()) {
                SimpleDoc simpleDoc = currentDocs.get(indexKey);
                if (simpleDoc == null) {
                    builder.put(indexKey, "record not found for update");

                } else if (longToCompare(simpleDoc.version()) != longToCompare(indexKey.version())) {
                    builder.put(indexKey, "version conflict for update");
                }
            } else if (currentDocs.containsKey(indexKey)) {
                builder.put(indexKey, "record already exists");
            }
        }
        checkValidity(builder.build());
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

        public Builder records(IndexRecord... records) {
            return records(asList(records));
        }

        public Builder records(Iterable<IndexRecord> records) {
            for (IndexRecord record : records) record(record);
            return this;
        }

        public Builder record(IndexRecord record) {
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
