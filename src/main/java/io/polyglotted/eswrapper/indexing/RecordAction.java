package io.polyglotted.eswrapper.indexing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.pgmodel.search.DocStatus;
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
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.filterKeys;
import static io.polyglotted.pgmodel.search.index.HiddenFields.*;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum RecordAction {
    CREATE {
        @Override
        public ActionRequest request(IndexRecord record, long timestamp, String user) {
            final String id = emptyToNull(record.id()); //auto-generate if empty
            log.debug("creating new record " + nonNull(id, "_auto_") + " for " + record.type() + " at " + record.index());

            return new IndexRequest(record.index(), record.type(), id).source(sourceOf(record, timestamp, user))
               .create(true).parent(record.parent()).versionType(VersionType.EXTERNAL).version(timestamp);
        }
    },
    UPDATE {
        @Override
        public ActionRequest request(IndexRecord record, long timestamp, String user) {
            log.debug("updating record " + record.id() + " for " + record.type() + " at " + record.index());

            return new IndexRequest(record.index(), record.type(), record.id()).source(sourceOf(record, timestamp, user))
               .parent(record.parent()).versionType(VersionType.EXTERNAL_GTE).version(timestamp);
        }
    },
    DELETE {
        @Override
        public ActionRequest request(IndexRecord record, long timestamp, String user) {
            log.debug("deleting record " + record.id() + " for " + record.type() + " at " + record.index());

            return new DeleteRequest(record.index(), record.type(), record.id());
        }
    };

    public abstract ActionRequest request(IndexRecord record, long timestamp, String user);

    public Map<String, Object> sourceFrom(SimpleDoc doc, DocStatus status, String comment, long timestamp, String user) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
           .putAll(filterKeys(doc.source, RecordAction::checkField)).put(STATUS_FIELD, status.name())
           .put(EXPIRY_FIELD, String.valueOf(timestamp)).put(UPDATER_FIELD, user);
        if (comment != null && !doc.hasItem(COMMENT_FIELD)) builder.put(COMMENT_FIELD, comment);
        return builder.build();
    }

    @VisibleForTesting
    static String sourceOf(IndexRecord record, long timestamp, String user) {
        StringBuilder builder = new StringBuilder();
        builder.append(record.source.substring(0, record.source.length() - 1));

        if (record.source.length() > 2) builder.append(",");
        if (record.isUpdate())
            builder.append("\"").append(ANCESTOR_FIELD).append("\":\"").append(record.uniqueId()).append("\",");
        if (record.status != null)
            builder.append("\"").append(STATUS_FIELD).append("\":\"").append(record.status.name()).append("\",");
        if (record.comment != null)
            builder.append("\"").append(COMMENT_FIELD).append("\":\"").append(record.comment).append("\",");
        if (record.baseVersion != null)
            builder.append("\"").append(BASEVERSION_FIELD).append("\":\"").append(record.baseVersion).append("\",");
        if (record.approvalRoles != null)
            builder.append("\"").append(APPROVAL_ROLES_FIELD).append("\":\"").append(record.approvalRoles).append("\",");

        builder.append("\"").append(BASEKEY_FIELD).append("\":\"").append(nonNull(record.id(), "_auto_")).append("\",");
        builder.append("\"").append(TIMESTAMP_FIELD).append("\":\"").append(timestamp).append("\"");
        builder.append(",\"").append(USER_FIELD).append("\":\"").append(user).append("\"");
        builder.append("}");

        return builder.toString();
    }

    static String nonNull(String nullable, String defVal) { return isNullOrEmpty(nullable) ? defVal : nullable; }

    static boolean checkField(String key) { return !STATUS_FIELD.equals(key); }
}
