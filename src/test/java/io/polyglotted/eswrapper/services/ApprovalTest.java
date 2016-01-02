package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexRecord;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.Indexable;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import io.polyglotted.pgmodel.search.Sleeve;
import io.polyglotted.pgmodel.search.index.FieldType;
import io.polyglotted.pgmodel.search.query.Expression;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.concat;
import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.updateRecord;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.IndexableHelper.approvalIndexable;
import static io.polyglotted.eswrapper.indexing.IndexableHelper.discardIndexable;
import static io.polyglotted.eswrapper.indexing.IndexableHelper.getPendingApprovals;
import static io.polyglotted.eswrapper.indexing.IndexableHelper.rejectionIndexable;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.services.IndexableTest.fetchRecords;
import static io.polyglotted.eswrapper.services.IndexableTest.newSleeveFunction;
import static io.polyglotted.eswrapper.services.Trade.FieldAddress;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.sampleTrades;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static io.polyglotted.pgmodel.search.DocStatus.DELETED;
import static io.polyglotted.pgmodel.search.DocStatus.DISCARDED;
import static io.polyglotted.pgmodel.search.DocStatus.EXPIRED;
import static io.polyglotted.pgmodel.search.DocStatus.LIVE;
import static io.polyglotted.pgmodel.search.DocStatus.PENDING;
import static io.polyglotted.pgmodel.search.DocStatus.PENDING_DELETE;
import static io.polyglotted.pgmodel.search.IndexKey.keyFrom;
import static io.polyglotted.pgmodel.search.Sleeve.createSleeves;
import static io.polyglotted.pgmodel.search.index.Alias.aliasBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.simpleField;
import static io.polyglotted.pgmodel.search.index.FieldType.LONG;
import static io.polyglotted.pgmodel.search.index.HiddenFields.BASEKEY_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.BASEVERSION_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.COMMENT_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.EXPIRY_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.STATUS_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.TIMESTAMP_FIELD;
import static io.polyglotted.pgmodel.search.query.Expressions.and;
import static io.polyglotted.pgmodel.search.query.Expressions.approvalRejected;
import static io.polyglotted.pgmodel.search.query.Expressions.archiveIndex;
import static io.polyglotted.pgmodel.search.query.Expressions.equalsTo;
import static io.polyglotted.pgmodel.search.query.Expressions.in;
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
    private static final long T1 = 1442784061000L;
    private static final long T2 = 1442784062000L;
    private static final long T3 = 1442784063000L;
    private static final long T4 = 1442784064000L;
    private static final long T5 = 1442784065000L;

    @Override
    protected void performSetup() {
        admin.dropIndex(APPROVAL_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), APPROVAL_INDEX);

        admin.createType(typeBuilder().index(APPROVAL_INDEX).type(TRADE_TYPE)
           .fieldMapping(notAnalyzedStringField(FieldAddress).build())
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());

        admin.createType(typeBuilder().index(APPROVAL_INDEX).type(APPROVAL_TYPE)
           .fieldMapping(simpleField(BASEVERSION_FIELD, LONG).docValues(null).includeInAll(false).build())
           .fieldMapping(notAnalyzedStringField(COMMENT_FIELD).docValues(null).includeInAll(false).build())
           .fieldMapping(notAnalyzedStringField(FieldAddress).build())
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());

        admin.updateAliases(aliasBuilder().alias(APPROVAL_LIVE).index(APPROVAL_INDEX).filter(liveIndex()).build(),
           aliasBuilder().alias(APPROVAL_HISTORY).index(APPROVAL_INDEX).filter(archiveIndex()).build());
    }

    @Test
    public void createAndApprove() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 5),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves, T1));
        assertLivePending(0, 5);

        indexer.twoPhaseCommit(approvalIndexable(query.getAll(originalKeys), "approved by test", "unit-approver", T2));
        assertLivePending(5, 0);
        assertHistory(5, T1, T2, LIVE.toStatus(), "approved by test", type(APPROVAL_TYPE));
    }

    @Test
    public void modificationAndApprove() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 10),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves, T1));
        indexer.twoPhaseCommit(approvalIndexable(query.getAll(originalKeys), "approved", "unit-approver", T2));

        List<IndexKey> modificationKeys = indexer.twoPhaseCommit(pendingIndexable(query, getMutations(), T3));
        assertLivePending(10, 5);

        indexer.twoPhaseCommit(approvalIndexable(query.getAll(modificationKeys), "approved mod", "unit-approver", T4));
        assertLivePending(11, 0);

        assertHistory(4, T3, T4, LIVE.toStatus(), "approved mod", and(type(APPROVAL_TYPE),
           in(BASEKEY_FIELD, "/trades/021", "/trades/022", "/trades/005", "/trades/010"), equalsTo(EXPIRY_FIELD, T4)));

        assertHistory(1, T3, T4, DELETED.toStatus(), "approved mod", and(type(APPROVAL_TYPE),
           in(BASEKEY_FIELD, "/trades/004"), equalsTo(EXPIRY_FIELD, T4)));
    }

    @Test
    public void rejectOnSave() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 3),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves, T1));
        assertLivePending(0, 3);

        indexer.twoPhaseCommit(rejectionIndexable(query.getAll(originalKeys), "rejected by test", "unit-approver", T2));
        assertLivePending(0, 0);
        assertRejected(3, T2, "rejected by test");
        assertHistory(3, T1, T2, EXPIRED.toStatus(), null, and(type(APPROVAL_TYPE),
           in(BASEKEY_FIELD, "/trades/001", "/trades/002", "/trades/003")));
    }

    @Test
    public void discardRejectedOrPending() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 3),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves, T1));
        List<IndexKey> rejectedKeys = indexer.twoPhaseCommit(rejectionIndexable(query.getAll(originalKeys),
           "rejected", "unit-approver", T2));
        assertRejected(3, T2, "rejected");

        List<Sleeve<Trade>> sleeves2 = createSleeves(sampleTrades().subList(3, 5),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> pendingKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves2, T1));
        assertLivePending(0, 2);

        indexer.twoPhaseCommit(discardIndexable(query.getAll(concat(rejectedKeys, pendingKeys)), "unit-tester", T3));
        assertLivePending(0, 0);
        assertRejected(0, 0, null);
        assertThat(fetchAll(type(APPROVAL_TYPE), equalsTo(STATUS_FIELD, DISCARDED.toStatus())).size(), is(5));
    }

    @Test
    public void editRejectedOrPendingAndApprove() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 3),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves, T1));
        List<IndexKey> rejectedKeys = indexer.twoPhaseCommit(rejectionIndexable(query.getAll(originalKeys),
           "rejected", "unit-approver", T2));

        List<Sleeve<Trade>> sleeves2 = createSleeves(sampleTrades().subList(3, 5),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> pendingKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves2, T1));

        List<IndexKey> editableKeys = ImmutableList.copyOf(concat(rejectedKeys, pendingKeys));
        List<Sleeve<Trade>> updSleeves = Lists.newArrayList();
        updSleeves.add(Sleeve.create(editableKeys.get(0),
           trade("/trades/001", "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 5.0)));
        updSleeves.add(Sleeve.create(editableKeys.get(1),
           trade("/trades/002", "EMEA", "UK", "London", "IEU", "Andrew", 1420848000000L, 10.0)));
        updSleeves.add(Sleeve.create(editableKeys.get(2),
           trade("/trades/003", "EMEA", "UK", "London", "IEU", "Bob", 1425427200000L, 15.0)));
        updSleeves.add(Sleeve.create(editableKeys.get(3),
           trade("/trades/004", "EMEA", "UK", "London", "NYM", "Charlie", 1423958400000L, 20.0)));
        updSleeves.add(Sleeve.create(editableKeys.get(4),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1422144000000L, 25.0)));

        List<IndexKey> updateKeys = indexer.twoPhaseCommit(pendingIndexable(query, updSleeves, T3));
        assertLivePending(0, 5);
        assertRejected(0, 0, null);

        indexer.twoPhaseCommit(approvalIndexable(query.getAll(updateKeys), "approved after upd", "unit-approver", T4));
        assertLivePending(5, 0);
        assertHistory(5, T3, T4, LIVE.toStatus(), "approved after upd", and(type(APPROVAL_TYPE), equalsTo(TIMESTAMP_FIELD,
           T3), in(BASEKEY_FIELD, "/trades/001", "/trades/002", "/trades/003", "/trades/004", "/trades/005")));
    }

    @Test
    public void editPendingAfterModification() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 10),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves, T1));
        indexer.twoPhaseCommit(approvalIndexable(query.getAll(originalKeys), "approved", "unit-approver", T2));

        List<IndexKey> modificationKeys = indexer.twoPhaseCommit(pendingIndexable(query, getMutations(), T3));
        assertLivePending(10, 5);

        List<Sleeve<Trade>> updSleeves = Lists.newArrayList();
        updSleeves.add(Sleeve.create(modificationKeys.get(2),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 20.0)));
        updSleeves.add(Sleeve.create(modificationKeys.get(3),
           trade("/trades/010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", 1425427200000L, 22.0)));
        List<IndexKey> updateKeys = indexer.twoPhaseCommit(pendingIndexable(query, updSleeves, T4));
        assertLivePending(10, 5);

        List<IndexKey> newApproveKeys = Lists.newArrayList();
        newApproveKeys.addAll(updateKeys);
        newApproveKeys.addAll(modificationKeys.subList(0, 2));
        newApproveKeys.add(modificationKeys.get(4));

        indexer.twoPhaseCommit(approvalIndexable(query.getAll(newApproveKeys), "new approval", "unit-approver", T5));
        assertLivePending(11, 0);
        assertHistory(2, T4, T5, LIVE.toStatus(), "new approval", and(type(APPROVAL_TYPE), equalsTo(TIMESTAMP_FIELD,
           T4), in(BASEKEY_FIELD, "/trades/005", "/trades/010")));
        assertHistory(2, T3, T5, LIVE.toStatus(), "new approval", and(type(APPROVAL_TYPE), equalsTo(TIMESTAMP_FIELD,
           T3), in(BASEKEY_FIELD, "/trades/021", "/trades/022")));
        assertHistory(1, T3, T5, DELETED.toStatus(), "new approval", and(type(APPROVAL_TYPE), equalsTo(TIMESTAMP_FIELD,
           T3), in(BASEKEY_FIELD, "/trades/004")));
    }

    @Test
    public void rejectEditAndApprove() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 10),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(query, sleeves, T1));
        indexer.twoPhaseCommit(approvalIndexable(query.getAll(originalKeys), "approved", "unit-approver", T2));

        List<IndexKey> modificationKeys = indexer.twoPhaseCommit(pendingIndexable(query, getMutations(), T3));
        List<IndexKey> rejectKeys = indexer.twoPhaseCommit(rejectionIndexable(query.getAll(modificationKeys),
           "rejected next", "unit approver", T4));
        assertLivePending(10, 0);
        assertRejected(5, T4, "rejected next");

        List<Sleeve<Trade>> updSleeves = Lists.newArrayList();
        updSleeves.add(Sleeve.create(rejectKeys.get(0),
           trade("/trades/021", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 42.0)));
        updSleeves.add(Sleeve.create(rejectKeys.get(3),
           trade("/trades/010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", 1425427200000L, 22.0)));
        List<IndexKey> updateKeys = indexer.twoPhaseCommit(pendingIndexable(query, updSleeves, T5));
        assertLivePending(10, 2);

        indexer.twoPhaseCommit(approvalIndexable(query.getAll(updateKeys), "new approval", "unit-approver", T5));
        assertLivePending(11, 0);
    }

    private List<SimpleDoc> fetchAll(Expression... exprs) {
        return fetchRecords(query, APPROVAL_INDEX, exprs);
    }

    private void assertLivePending(int liveRecords, int pending) {
        assertThat(fetchRecords(query, APPROVAL_LIVE).size(), is(liveRecords));
        assertThat(fetchRecords(query, APPROVAL_INDEX, pendingApproval()).size(), is(pending));
    }

    private void assertRejected(int size, long version, String comment) {
        List<SimpleDoc> simpleDocs = fetchRecords(query, APPROVAL_INDEX, approvalRejected());
        assertThat(simpleDocs.size(), is(size));
        for (SimpleDoc doc : simpleDocs) {
            assertThat(doc.version(), is(version));
            assertThat(doc.strVal(COMMENT_FIELD), is(comment));
        }
    }

    private void assertHistory(int size, long version, long expiry, String status, String comment, Expression... exprs) {
        List<SimpleDoc> simpleDocs = fetchRecords(query, APPROVAL_HISTORY, exprs);
        assertThat(simpleDocs.size(), is(size));
        for (SimpleDoc doc : simpleDocs) {
            assertThat(doc.version(), is(version));
            assertThat(doc.longStrVal(EXPIRY_FIELD), is(expiry));
            assertThat(doc.strVal(STATUS_FIELD), is(status));
            assertThat(doc.strVal(COMMENT_FIELD), is(comment));
        }
    }

    private static Indexable pendingIndexable(QueryWrapper query, List<Sleeve<Trade>> sleeves, long ts) {
        Map<IndexKey, SimpleDoc> docMap = getPendingApprovals(query, sleeves);
        Indexable.Builder builder = indexableBuilder().user("unit-tester").timestamp(ts);
        for (Sleeve<Trade> sleeve : sleeves) {
            IndexRecord.Builder record;
            if (docMap.containsKey(sleeve.key)) {
                record = updateRecord(sleeve.key).baseVersion(docMap.get(sleeve.key).baseVersion());
            } else {
                record = createRecord(sleeve.approvalKey()).baseVersion(sleeve.version());
            }
            if (sleeve.shouldDelete()) {
                record.status(PENDING_DELETE).source("{}");
            } else {
                record.status(PENDING).source(GSON.toJson(sleeve.source));
            }
            builder.record(record.build());
        }
        return builder.build();
    }

    private static List<Sleeve<Trade>> getMutations() {
        List<Sleeve<Trade>> mutations = Lists.newArrayList();
        mutations.addAll(createSleeves(ImmutableList.of(
              trade("/trades/021", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0),
              trade("/trades/022", "EMEA", "UK", "London", "IEU", "Andrew", 1420848000000L, 5.0)),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE)));
        mutations.add(Sleeve.create(keyFrom(APPROVAL_INDEX, TRADE_TYPE, "/trades/005", T2),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0)));
        mutations.add(Sleeve.create(keyFrom(APPROVAL_INDEX, TRADE_TYPE, "/trades/010", T2),
           trade("/trades/010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", 1425427200000L, 16.0)));
        mutations.add(Sleeve.delete(keyFrom(APPROVAL_INDEX, TRADE_TYPE, "/trades/004", T2)));
        return mutations;
    }

}
