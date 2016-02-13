package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.copyOf;
import static io.polyglotted.pgmodel.search.DocStatus.DELETED;
import static io.polyglotted.pgmodel.search.DocStatus.EXPIRED;
import static io.polyglotted.pgmodel.search.IndexKey.keyWith;
import static io.polyglotted.pgmodel.util.ModelUtil.equalsAll;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class IndexRecord {
    @Delegate(excludes = KeyExclude.class)
    public final IndexKey indexKey;
    public final RecordAction action;
    public final DocStatus status;
    public final DocStatus updateStatus;
    public final String ancestor;
    public final Long baseVersion;
    public final String comment;
    public final String updateComment;
    public final String source;
    public final ImmutableList<String> approvalRoles;

    public static <T> IndexRecord fromSleeve(Sleeve<T> sleeve, Function<Sleeve<T>, String> function) {
        return (sleeve.isNew()) ? createRecord(sleeve.key, function.apply(sleeve)) : (sleeve.shouldDelete() ?
           deleteRecord(sleeve.key) : updateRecord(sleeve.key, function.apply(sleeve)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexRecord that = (IndexRecord) o;
        return equalsAll(idOrRandom(), that.idOrRandom(), indexKey, that.indexKey, action, that.action);
    }

    @Override
    public int hashCode() { return Objects.hash(indexKey, action); }

    private String idOrRandom() { return isNullOrEmpty(id()) ? randomUUID().toString() : id(); }

    public IndexKey key() { return indexKey; }

    public boolean isUpdate() { return action != RecordAction.CREATE && ancestor != null; }

    public ActionRequest request(long timestamp, String user) { return action.request(this, timestamp, user); }

    public static IndexRecord overwriteRecord(IndexKey key, String source) {
        return new Builder(checkNotNull(key, "key cannot be null"), RecordAction.OVERWRITE).source(source).build();
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

    public static IndexRecord updateRecord(IndexKey key, DocStatus status, String comment, String source) {
        return updateRecord(key).status(status).comment(comment).source(source).build();
    }

    public static Builder updateRecord(IndexKey key) {
        return new Builder(checkNotNull(key, "key cannot be null"), RecordAction.UPDATE)
           .updateStatus(EXPIRED).ancestor(key.uniqueId());
    }

    public static IndexRecord deleteRecord(IndexKey key) {
        return deleteRecord(key, null, DELETED);
    }

    public static IndexRecord deleteRecord(IndexKey key, String updateComment, DocStatus updateStatus) {
        return new Builder(checkNotNull(key, "key cannot be null").delete(), RecordAction.DELETE)
           .updateStatus(updateStatus).ancestor(key.uniqueId()).updateComment(updateComment).source("").build();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        public final IndexKey indexKey;
        public final RecordAction action;
        private DocStatus status;
        private DocStatus updateStatus;
        private String ancestor;
        private Long baseVersion;
        private String comment;
        private String updateComment;
        private String source;
        private final List<String> approvalRoles = new ArrayList<>();

        public Builder approvalRoles(String... roles) {
            return roles.length == 0 ? this : (roles.length == 1 ? approvalRole(roles[0]) : approvalRoles(asList(roles)));
        }

        public Builder approvalRoles(Iterable<String> approvalRoles) {
            Iterables.addAll(this.approvalRoles, approvalRoles);
            return this;
        }

        public Builder approvalRole(String approvalRole) {
            this.approvalRoles.add(approvalRole);
            return this;
        }

        public IndexRecord build() {
            return new IndexRecord(checkNotNull(indexKey, "key cannot be null"), action, status, updateStatus, ancestor,
               baseVersion, comment, updateComment, checkNotNull(source, "source is required"), copyOf(approvalRoles));
        }
    }
}
