package io.polyglotted.eswrapper.indexing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.pgmodel.search.SimpleDoc;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import java.util.Map;

import static com.google.common.base.Strings.emptyToNull;
import static io.polyglotted.pgmodel.search.index.HiddenFields.ANCESTOR_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.EXPIRY_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.STATUS_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.TIMESTAMP_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.UPDATER_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.USER_FIELD;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum RecordAction {
    CREATE("new") {
        @Override
        public ActionRequest request(IndexRecord record, long timestamp, String user) {
            final String id = emptyToNull(record.id()); //auto-generate if empty
            log.debug("creating new record " + id + " for " + record.type() + " at " + record.index());

            return new IndexRequest(record.index(), record.type(), id).source(sourceOf(record, timestamp, user))
               .create(true).parent(record.parent()).versionType(VersionType.EXTERNAL).version(timestamp);
        }
    },
    UPDATE("expired") {
        @Override
        public ActionRequest request(IndexRecord record, long timestamp, String user) {
            log.debug("updating record " + record.id() + " for " + record.type() + " at " + record.index());

            return new IndexRequest(record.index(), record.type(), record.id()).source(sourceOf(record, timestamp, user))
               .parent(record.parent()).versionType(VersionType.EXTERNAL_GTE).version(timestamp);
        }
    },
    DELETE("deleted") {
        @Override
        public ActionRequest request(IndexRecord record, long timestamp, String user) {
            log.debug("deleting record " + record.id() + " for " + record.type() + " at " + record.index());

            return new DeleteRequest(record.index(), record.type(), record.id());
        }
    };

    public final String status;

    public abstract ActionRequest request(IndexRecord record, long timestamp, String user);

    public Map<String, Object> sourceFrom(SimpleDoc simpleDoc, long timestamp, String user) {
        return ImmutableMap.<String, Object>builder().putAll(simpleDoc.source).put(STATUS_FIELD, status)
           .put(EXPIRY_FIELD, String.valueOf(timestamp)).put(UPDATER_FIELD, user).build();
    }

    @VisibleForTesting
    static String sourceOf(IndexRecord record, long timestamp, String user) {
        StringBuilder builder = new StringBuilder();
        builder.append(record.source.substring(0, record.source.length() - 1));

        if (record.source.length() > 2) builder.append(",");
        if (record.isUpdate()) {
            builder.append("\"").append(ANCESTOR_FIELD).append("\":\"").append(record.uniqueId()).append("\",");
        }
        builder.append("\"").append(TIMESTAMP_FIELD).append("\":\"").append(timestamp).append("\"");
        builder.append(",\"").append(USER_FIELD).append("\":\"").append(user).append("\"");
        builder.append("}");

        return builder.toString();
    }
}
