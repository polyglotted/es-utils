package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.query.response.SimpleDoc;
import io.polyglotted.eswrapper.services.IndexerException;
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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableList.copyOf;

@Slf4j
@RequiredArgsConstructor
public final class Indexable {
    public final String index;
    public final long timestamp;
    public final ImmutableList<IndexRecord> records;

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

            String uniqueId = record.uniqueId();
            log.debug("creating archive record " + uniqueId + " for " + record.id() + " at " + index);

            request.add(new IndexRequest(index, record.type(), uniqueId).create(true)
               .versionType(VersionType.EXTERNAL).version(record.version())
               .source(mapFrom(currentDocs.get(record.indexKey), record.action, timestamp)));
        }
        return request;
    }

    public BulkRequest writeRequest() {
        return new BulkRequest().refresh(false).add(transform(records, record -> record.request(index, timestamp)));
    }

    private static Map<String, Object> mapFrom(SimpleDoc simpleDoc, IndexRecord.Action action, long timestamp) {
        return ImmutableMap.<String, Object>builder().putAll(simpleDoc.source)
           .put("&status", action.status).put("&expiry", timestamp).build();
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
        private String index;
        private long timestamp = System.currentTimeMillis();
        private final Set<IndexRecord> records = new LinkedHashSet<>();

        public Builder records(Iterable<IndexRecord> records) {
            for (IndexRecord record : records)
                checkArgument(this.records.add(record), "record already exists {}", record.indexKey);
            return this;
        }

        public Indexable build() {
            return new Indexable(checkNotNull(index, "index cannot be null"), timestamp, copyOf(records));
        }
    }
}
