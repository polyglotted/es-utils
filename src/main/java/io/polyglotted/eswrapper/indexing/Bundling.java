package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;

@Slf4j
@RequiredArgsConstructor
public final class Bundling {
    public final ImmutableList<IndexRecord> records;
    public final long timestamp;
    public final String user;

    public BulkRequest writeRequest() {
        return new BulkRequest().refresh(false).add(transform(records, record -> record.request(timestamp, user)));
    }

    public String[] indices() {
        return toArray(newHashSet(transform(records, IndexRecord::index)), String.class);
    }

    public static Builder bundlingBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private long timestamp = System.currentTimeMillis();
        private String user = "System";
        private final List<IndexRecord> records = new ArrayList<>();

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

        public Bundling build() {
            return new Bundling(copyOf(records), timestamp, user);
        }
    }
}
