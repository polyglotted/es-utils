package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.polyglotted.eswrapper.services.QueryWrapper;
import io.polyglotted.pgmodel.search.DocStatus;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import io.polyglotted.pgmodel.search.Sleeve;
import io.polyglotted.pgmodel.search.query.QueryResponse;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.deleteRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.updateRecord;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.query.QueryBuilder.idRequest;
import static io.polyglotted.eswrapper.query.QueryBuilder.toStrArray;
import static io.polyglotted.eswrapper.query.ResultBuilder.SimpleDocBuilder;
import static io.polyglotted.pgmodel.search.index.HiddenFields.APPROVAL_ROLES_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.BASEVERSION_FIELD;

public abstract class IndexableHelper {
    private static Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    public static Indexable approvalIndexable(Iterable<SimpleDoc> docs, String comment, String user, long timestamp) {
        Indexable.Builder builder = Indexable.indexableBuilder().user(user).timestamp(timestamp);
        for (SimpleDoc doc : docs) {
            IndexKey baseKey = doc.baseKey(doc.baseVersion());

            if (doc.status() == DocStatus.PENDING_DELETE) {
                builder.record(deleteRecord(doc.key, comment, DocStatus.DELETED));
                builder.record(deleteRecord(baseKey));
            } else {
                builder.record(deleteRecord(doc.key, comment, DocStatus.LIVE));
                if (doc.hasItem(BASEVERSION_FIELD)) {
                    builder.record(updateRecord(baseKey, GSON.toJson(doc.filteredCopy(false))));
                } else {
                    builder.record(createRecord(baseKey, GSON.toJson(doc.filteredCopy(false))));
                }
            }
        }
        return builder.build();
    }

    public static Indexable rejectionIndexable(Iterable<SimpleDoc> docs, String comment, String user, long timestamp) {
        Indexable.Builder builder = Indexable.indexableBuilder().user(user).timestamp(timestamp);
        for (SimpleDoc doc : docs)
            builder.record(updateRecord(doc.key, DocStatus.REJECTED, comment, GSON.toJson(doc.filteredCopy(true))));
        return builder.build();
    }

    public static Indexable discardIndexable(Iterable<SimpleDoc> docs, String user, long timestamp) {
        Indexable.Builder builder = Indexable.indexableBuilder().user(user).timestamp(timestamp);
        for (SimpleDoc doc : docs) builder.record(deleteRecord(doc.key, null, DocStatus.DISCARDED));
        return builder.build();
    }

    public static <T> Map<IndexKey, SimpleDoc> getPendingApprovals(QueryWrapper query, Iterable<Sleeve<T>> sleeves) {
        Set<String> indices = ImmutableSet.copyOf(transform(sleeves, Sleeve::index));
        Set<String> types = ImmutableSet.copyOf(transform(sleeves, Sleeve::approvalType));
        QueryResponse queryResponse = query.search(idRequest(toStrArray(transform(sleeves, Sleeve::id)),
           types, toStrArray(indices)), SimpleDocBuilder);
        return Maps.uniqueIndex(queryResponse.resultsAs(SimpleDoc.class), SimpleDoc::key);
    }

    public static boolean validateApprovalRoles(List<SimpleDoc> docs, List<String> userRoles) {
        for (SimpleDoc doc : docs) {
            String typeRoles = doc.strVal(APPROVAL_ROLES_FIELD);
            if (isNullOrEmpty(typeRoles)) continue;

            List<String> approvalRoles = newArrayList(COMMA_SPLITTER.split(typeRoles));
            approvalRoles.retainAll(userRoles);
            if (approvalRoles.isEmpty()) return false;
        }
        return true;
    }
}
