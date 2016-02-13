package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.indexing.IndexRecord;
import io.polyglotted.eswrapper.indexing.Indexable;
import io.polyglotted.pgmodel.search.DocStatus;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.uniqueIndex;
import static io.polyglotted.eswrapper.services.ValidityException.checkValidity;
import static io.polyglotted.pgmodel.search.KeyUtil.longToCompare;

public interface VersionValidator {

    BulkRequest validate(Indexable indexable, List<SimpleDoc> docs);

    VersionValidator STANDARD_VALIDATOR = new StandardValidator();
    VersionValidator OVERWRITE_VALIDATOR = new OverwriteValidator();

    @Slf4j
    class StandardValidator implements VersionValidator {

        @Override
        public BulkRequest validate(Indexable indexable, List<SimpleDoc> docs) {
            Map<String, SimpleDoc> currentDocMap = uniqueIndex(docs, SimpleDoc::baseIndexId);
            prevalidateCurrentDocs(indexable.keys());
            validateCurrentDocs(indexable.records, currentDocMap);

            BulkRequest request = new BulkRequest().refresh(false);
            for (IndexRecord record : indexable.records) {
                if (!record.isUpdate()) continue;

                SimpleDoc simpleDoc = currentDocMap.get(record.baseIndexId());
                log.debug("creating archive record " + simpleDoc.uniqueId() + " for " + record.id()
                   + " for type " + record.type() + " at " + record.index());

                request.add(new IndexRequest(simpleDoc.index(), simpleDoc.type(), simpleDoc.uniqueId()).create(true)
                   .parent(record.parent()).versionType(VersionType.EXTERNAL).version(record.version())
                   .source(record.action.sourceFrom(simpleDoc, record.updateStatus, record.updateComment,
                      indexable.timestamp, indexable.user)));
            }
            return request;
        }

        private void validateCurrentDocs(List<IndexRecord> records, Map<String, SimpleDoc> currentDocs) {
            ImmutableMap.Builder<IndexKey, String> errors = ImmutableMap.builder();
            for (IndexRecord record : records) {
                String baseIndexId = record.baseIndexId();

                if (record.isUpdate()) {
                    SimpleDoc simpleDoc = currentDocs.get(baseIndexId);
                    if (simpleDoc == null) {
                        errors.put(record.indexKey, "record not found for update");

                    } else if (longToCompare(simpleDoc.version()) != longToCompare(record.version())) {
                        errors.put(record.indexKey, "version conflict for update");
                    }
                } else if (currentDocs.containsKey(baseIndexId)) {
                    errors.put(record.indexKey, "record already exists");
                }
            }
            checkValidity(errors.build());
        }

        @SuppressWarnings("unused")
        protected void prevalidateCurrentDocs(Collection<IndexKey> indexKeys) {}
    }

    @Slf4j
    final class OverwriteValidator implements VersionValidator {

        @Override
        public BulkRequest validate(Indexable indexable, List<SimpleDoc> docs) {
            Map<String, SimpleDoc> currentDocs = uniqueIndex(docs, SimpleDoc::baseIndexId);
            BulkRequest request = new BulkRequest().refresh(false);

            for (IndexRecord record : indexable.records) {
                SimpleDoc simpleDoc = currentDocs.get(record.baseIndexId());
                if (simpleDoc == null) continue;

                log.debug("creating archive record " + simpleDoc.uniqueId() + " for " + record.id()
                   + " for type " + record.type() + " at " + record.index());

                request.add(new IndexRequest(record.index(), record.type(), simpleDoc.uniqueId()).create(true)
                   .parent(record.parent()).versionType(VersionType.EXTERNAL).version(simpleDoc.version())
                   .source(record.action.sourceFrom(simpleDoc, DocStatus.EXPIRED, record.updateComment,
                      indexable.timestamp, indexable.user)));
            }
            return request;
        }
    }
}
