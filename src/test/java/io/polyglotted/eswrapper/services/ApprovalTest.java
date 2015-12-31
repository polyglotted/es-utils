package io.polyglotted.eswrapper.services;

import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexRecord;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.Indexable;
import io.polyglotted.pgmodel.search.DocStatus;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import io.polyglotted.pgmodel.search.Sleeve;
import io.polyglotted.pgmodel.search.index.FieldType;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.IndexRecord.forApproval;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.approvalIndexable;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.services.IndexableTest.fetchRecords;
import static io.polyglotted.eswrapper.services.IndexableTest.newSleeveFunction;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.sampleTrades;
import static io.polyglotted.pgmodel.search.Sleeve.createSleeves;
import static io.polyglotted.pgmodel.search.index.Alias.aliasBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.simpleField;
import static io.polyglotted.pgmodel.search.index.FieldType.LONG;
import static io.polyglotted.pgmodel.search.index.HiddenFields.BASEVERSION_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.COMMENT_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.EXPIRY_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.STATUS_FIELD;
import static io.polyglotted.pgmodel.search.query.Expressions.archiveIndex;
import static io.polyglotted.pgmodel.search.query.Expressions.liveIndex;
import static io.polyglotted.pgmodel.search.query.Expressions.pendingApproval;
import static io.polyglotted.pgmodel.search.query.Expressions.type;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ApprovalTest extends AbstractElasticTest {
    private static final String APPROVAL_INDEX = "approval_index";
    private static final String APPROVAL_LIVE = "approval_index.live";
    private static final String APPROVAL_HISTORY = "approval_index.history";
    private static final String APPROVAL_TYPE = TRADE_TYPE + "$approval";
    private static final long T1 = 1442784057000L;
    private static final long T2 = 1442784062000L;

    @Override
    protected void performSetup() {
        admin.dropIndex(APPROVAL_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), APPROVAL_INDEX);

        admin.createType(typeBuilder().index(APPROVAL_INDEX).type(TRADE_TYPE)
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());
        admin.createType(typeBuilder().index(APPROVAL_INDEX).type(APPROVAL_TYPE)
           .fieldMapping(simpleField(BASEVERSION_FIELD, LONG).docValues(null).includeInAll(false).build())
           .fieldMapping(notAnalyzedStringField(COMMENT_FIELD).docValues(null).includeInAll(false).build())
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());

        admin.updateAliases(aliasBuilder().alias(APPROVAL_LIVE).index(APPROVAL_INDEX).filter(liveIndex()).build(),
           aliasBuilder().alias(APPROVAL_HISTORY).index(APPROVAL_INDEX).filter(archiveIndex()).build());
    }

    @Test
    public void createAndApprove() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 5),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));

        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(sleeves, T1));
        assertLivePending(0, 5);

        indexer.twoPhaseCommit(approvalIndexable(query.getAll(originalKeys), "approved by test", "unit-approver", T2));
        assertLivePending(5, 0);
        assertHistory(5, T1, T2, DocStatus.LIVE.toStatus(), "approved by test");
    }

    private void assertLivePending(int liveRecords, int pending) {
        assertThat(fetchRecords(query, APPROVAL_LIVE).size(), is(liveRecords));
        assertThat(fetchRecords(query, APPROVAL_INDEX, pendingApproval()).size(), is(pending));
    }

    private void assertHistory(int size, long version, long expiry, String status, String comment) {
        List<SimpleDoc> simpleDocs = fetchRecords(query, APPROVAL_HISTORY, type(APPROVAL_TYPE));
        assertThat(simpleDocs.size(), is(size));
        for(SimpleDoc doc : simpleDocs) {
            assertThat(doc.version(), is(version));
            assertThat(doc.longStrVal(EXPIRY_FIELD), is(expiry));
            assertThat(doc.strVal(STATUS_FIELD), is(status));
            assertThat(doc.strVal(COMMENT_FIELD), is(comment));
        }
    }

    private static Indexable pendingIndexable(Iterable<Sleeve<Trade>> sleeveDocs, long ts) {
        return indexableBuilder().user("unit-tester").timestamp(ts)
           .records(sleeveToRecords(sleeveDocs)).build();
    }

    private static Iterable<IndexRecord> sleeveToRecords(Iterable<Sleeve<Trade>> sleeveDocs) {
        return transform(sleeveDocs, doc -> forApproval(doc, sl -> GSON.toJson(sl.source)));
    }
}
