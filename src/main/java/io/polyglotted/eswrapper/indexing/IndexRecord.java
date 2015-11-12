package io.polyglotted.eswrapper.indexing;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.eswrapper.indexing.FieldMapping.ANCESTOR_FIELD;
import static io.polyglotted.eswrapper.indexing.FieldMapping.TIMESTAMP_FIELD;
import static io.polyglotted.eswrapper.indexing.IndexKey.keyWith;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class IndexRecord {
    @Delegate(excludes = KeyExclude.class)
    public final IndexKey indexKey;
    public final Action action;
    public final String source;
    public final String ancestor;

    public IndexKey key() {
        return indexKey;
    }

    public boolean isUpdate() {
        return action != Action.CREATE;
    }

    public ActionRequest request(String index, long timestamp) {
        return action.request(this, index, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) && indexKey.equals(((IndexRecord) o).indexKey));
    }

    @Override
    public int hashCode() {
        return 19 * indexKey.hashCode();
    }

    @Slf4j
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public enum Action {
        CREATE("new") {
            @Override
            public ActionRequest request(IndexRecord record, String index, long timestamp) {
                log.debug("creating new record " + record.id() + " at " + index);
                return new IndexRequest(index, record.type(), record.id()).source(sourceOf(record, timestamp))
                   .create(true).parent(record.parent()).versionType(VersionType.EXTERNAL).version(timestamp);
            }
        },
        UPDATE("expired") {
            @Override
            public ActionRequest request(IndexRecord record, String index, long timestamp) {
                log.debug("updating record " + record.id() + " at " + index);
                return new IndexRequest(index, record.type(), record.id()).source(sourceOf(record, timestamp))
                   .parent(record.parent()).versionType(VersionType.EXTERNAL_GTE).version(timestamp);
            }
        },
        DELETE("deleted") {
            @Override
            public ActionRequest request(IndexRecord record, String index, long timestamp) {
                log.debug("deleting record " + record.id() + " at " + index);
                return new DeleteRequest(index, record.type(), record.id());
            }
        };

        public final String status;

        public abstract ActionRequest request(IndexRecord record, String index, long timestamp);
    }

    @VisibleForTesting
    static String sourceOf(IndexRecord record, long timestamp) {
        StringBuilder builder = new StringBuilder();
        builder.append(record.source.substring(0, record.source.length() - 1));
        if(record.source.length() > 2) builder.append(",");
        builder.append("\"").append(TIMESTAMP_FIELD).append("\":").append(timestamp);
        if (record.ancestor != null) {
            builder.append(",\"").append(ANCESTOR_FIELD).append("\":\"").append(record.ancestor).append("\"");
        }
        return builder.append("}").toString();
    }

    public static Builder createRecord(String type, String location) {
        return createRecord(keyWith(checkNotNull(type), checkNotNull(location)));
    }

    public static Builder createRecord(IndexKey key) {
        return new Builder(checkNotNull(key, "key cannot be null"), Action.CREATE);
    }

    public static Builder updateRecord(IndexKey key) {
        return new Builder(checkNotNull(key, "key cannot be null"), Action.UPDATE);
    }

    public static IndexRecord deleteRecord(IndexKey key) {
        return new Builder(checkNotNull(key, "key cannot be null").delete(), Action.DELETE).source("").build();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        public final IndexKey indexKey;
        public final Action action;
        private String source;
        private String ancestor;

        public IndexRecord build() {
            return new IndexRecord(checkNotNull(indexKey, "key cannot be null"), action,
               checkNotNull(source, "source cannot be null"), ancestor);
        }
    }

    @SuppressWarnings("unused")
    private interface KeyExclude {
        String index();

        int compareTo(IndexKey other);

        IndexKey delete();

        IndexKey version(long version);
    }
}
