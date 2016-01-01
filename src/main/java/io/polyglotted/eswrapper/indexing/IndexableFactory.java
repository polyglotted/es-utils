package io.polyglotted.eswrapper.indexing;

import io.polyglotted.pgmodel.search.DocStatus;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;

import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.deleteRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.updateRecord;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.pgmodel.search.index.HiddenFields.BASEVERSION_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.STATUS_FIELD;

public abstract class IndexableFactory {

    public static Indexable approvalIndexable(Iterable<SimpleDoc> docs, String comment, String user, long timestamp) {
        Indexable.Builder builder = Indexable.indexableBuilder().user(user).timestamp(timestamp);
        for (SimpleDoc doc : docs) {
            DocStatus status = DocStatus.fromStatus(doc.strVal(STATUS_FIELD));
            Long baseVersion = doc.hasItem(BASEVERSION_FIELD) ? doc.longStrVal(BASEVERSION_FIELD) : null;
            IndexKey baseKey = doc.baseKey(baseVersion);

            if (status == DocStatus.PENDING_DELETE) {
                builder.record(deleteRecord(doc.key, comment, DocStatus.DELETED));
                builder.record(deleteRecord(baseKey));
            } else {
                builder.record(deleteRecord(doc.key, comment, DocStatus.LIVE));
                if (baseVersion == null) {
                    builder.record(createRecord(baseKey, GSON.toJson(doc.filteredCopy())));
                } else {
                    builder.record(updateRecord(baseKey, GSON.toJson(doc.filteredCopy())));
                }
            }
        }
        return builder.build();
    }

    public static Indexable rejectionIndexable(Iterable<SimpleDoc> docs, String comment, String user, long timestamp) {
        Indexable.Builder builder = Indexable.indexableBuilder().user(user).timestamp(timestamp);
        for (SimpleDoc doc : docs)
            builder.record(updateRecord(doc.key, DocStatus.REJECTED, comment, GSON.toJson(doc.filteredCopy())));
        return builder.build();
    }

    public static Indexable discardIndexable(Iterable<SimpleDoc> docs, String user, long timestamp) {
        Indexable.Builder builder = Indexable.indexableBuilder().user(user).timestamp(timestamp);
        for (SimpleDoc doc : docs) builder.record(deleteRecord(doc.key, null, DocStatus.DISCARDED));
        return builder.build();
    }
}
