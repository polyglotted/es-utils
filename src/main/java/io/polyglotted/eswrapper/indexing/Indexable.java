package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.polyglotted.pgmodel.search.IndexKey;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.getFirst;
import static java.util.Arrays.asList;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Indexable {
    public final String unaryIndex;
    public final ImmutableList<IndexRecord> records;
    public final long timestamp;
    public final String user;
    private volatile Collection<IndexKey> _keys;

    public Collection<IndexKey> keys() {
        return _keys == null ? (_keys = transform(records, IndexRecord::key)) : _keys;
    }

    public BulkRequest writeRequest() {
        return new BulkRequest().refresh(false).add(transform(records, record -> record.request(timestamp, user)));
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
            return records.length == 1 ? record(records[0]) : records(asList(records));
        }

        public Builder records(IndexRecord.Builder... builders) {
            return builders.length == 1 ? record(builders[0].build()) :
               records(transform(asList(builders), IndexRecord.Builder::build));
        }

        public Builder records(Iterable<IndexRecord> records) {
            for (IndexRecord record : records) record(record);
            return this;
        }

        public Builder record(IndexRecord.Builder builder) {
            return record(builder.build());
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
