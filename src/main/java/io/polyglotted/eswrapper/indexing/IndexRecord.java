package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Function;
import io.polyglotted.pgmodel.search.DocStatus;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.KeyExclude;
import io.polyglotted.pgmodel.search.Sleeve;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import org.elasticsearch.action.ActionRequest;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.pgmodel.search.DocStatus.DELETED;
import static io.polyglotted.pgmodel.search.DocStatus.EXPIRED;
import static io.polyglotted.pgmodel.search.DocStatus.PENDING;
import static io.polyglotted.pgmodel.search.DocStatus.PENDING_DELETE;
import static io.polyglotted.pgmodel.search.IndexKey.keyWith;
import static io.polyglotted.pgmodel.util.ModelUtil.equalsAll;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class IndexRecord {
    @Delegate(excludes = KeyExclude.class)
    public final IndexKey indexKey;
    public final RecordAction action;
    public final DocStatus status;
    public final DocStatus updateStatus;
    public final String source;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexRecord that = (IndexRecord) o;
        return equalsAll(indexKey, that.indexKey, action, that.action);
    }

    @Override
    public int hashCode() { return Objects.hash(indexKey, action); }

    public IndexKey key() { return indexKey; }

    public boolean isNew() { return action == RecordAction.CREATE; }

    public boolean isUpdate() { return action != RecordAction.CREATE; }

    public boolean hasStatus() { return status != null; }

    public ActionRequest request(long timestamp, String user) { return action.request(this, timestamp, user); }

    public static <T> IndexRecord fromSleeve(Sleeve<T> sleeve, Function<Sleeve<T>, String> function) {
        return (sleeve.isNew()) ? createRecord(sleeve.key, function.apply(sleeve)) : (sleeve.shouldDelete() ?
           deleteRecord(sleeve.key) : updateRecord(sleeve.key, function.apply(sleeve)));
    }

    public static <T> IndexRecord forApproval(Sleeve<T> sleeve, Function<Sleeve<T>, String> function) {
        return (sleeve.isNew()) ? createRecord(sleeve.key).status(PENDING).source(function.apply(sleeve)).build() :
           (sleeve.shouldDelete() ? createRecord(sleeve.key).status(PENDING_DELETE).source("").build()
           : updateRecord(sleeve.key).status(PENDING).source(function.apply(sleeve)).build());
    }

    public static IndexRecord createRecord(IndexKey key, String source) {
        return createRecord(key).source(source).build();
    }

    public static Builder createRecord(String index, String type, String id) {
        return createRecord(keyWith(index, type, id));
    }

    public static Builder createRecord(IndexKey key) {
        return new Builder(checkNotNull(key, "key cannot be null"), RecordAction.CREATE);
    }

    public static IndexRecord updateRecord(IndexKey key, String source) {
        return updateRecord(key).source(source).build();
    }

    public static Builder updateRecord(IndexKey key) {
        return new Builder(checkNotNull(key, "key cannot be null"), RecordAction.UPDATE).updateStatus(EXPIRED);
    }

    public static IndexRecord deleteRecord(IndexKey key) {
        return new Builder(checkNotNull(key, "key cannot be null").delete(), RecordAction.DELETE)
           .updateStatus(DELETED).source("").build();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        public final IndexKey indexKey;
        public final RecordAction action;
        private DocStatus status;
        private DocStatus updateStatus;
        private String source;

        public IndexRecord build() {
            return new IndexRecord(checkNotNull(indexKey, "key cannot be null"), action, status, updateStatus,
               checkNotNull(source, "source cannot be null"));
        }
    }
}
