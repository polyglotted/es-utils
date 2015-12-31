package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.IndexRecord.forApproval;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.approvalIndexable;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.Indexable.rejectionIndexable;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.services.IndexableTest.fetchRecords;
import static io.polyglotted.eswrapper.services.IndexableTest.newSleeveFunction;
import static io.polyglotted.eswrapper.services.Trade.FieldAddress;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.sampleTrades;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static io.polyglotted.pgmodel.search.DocStatus.DELETED;
import static io.polyglotted.pgmodel.search.DocStatus.EXPIRED;
import static io.polyglotted.pgmodel.search.DocStatus.LIVE;
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
    private static final long T1 = 1442784057000L;
    private static final long T2 = 1442784062000L;
    private static final long T3 = 1442784063000L;
    private static final long T4 = 1442784064000L;

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
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(sleeves, T1));
        assertLivePending(0, 5);

        indexer.twoPhaseCommit(approvalIndexable(query.getAll(originalKeys), "approved by test", "unit-approver", T2));
        assertLivePending(5, 0);
        assertHistory(5, T1, T2, LIVE.toStatus(), "approved by test", type(APPROVAL_TYPE));
    }

    @Test
    public void modificationAndApprove() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades().subList(0, 10),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE));
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(sleeves, T1));
        indexer.twoPhaseCommit(approvalIndexable(query.getAll(originalKeys), "approved", "unit-approver", T2));

        List<IndexKey> modificationKeys = indexer.twoPhaseCommit(pendingIndexable(getMutations(), T3));
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
        List<IndexKey> originalKeys = indexer.twoPhaseCommit(pendingIndexable(sleeves, T1));
        assertLivePending(0, 3);

        indexer.twoPhaseCommit(rejectionIndexable(query.getAll(originalKeys), "rejected by test", "unit-approver", T2));
        assertLivePending(0, 0);
        assertRejected(3, T2, "rejected by test");
        assertHistory(3, T1, T2, EXPIRED.toStatus(), null, and(type(APPROVAL_TYPE),
           in(BASEKEY_FIELD, "/trades/001", "/trades/002", "/trades/003")));
    }

    private void printAll(Expression... exprs) {
        fetchRecords(query, APPROVAL_INDEX, exprs).forEach(System.out::println);
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

    private static Indexable pendingIndexable(Iterable<Sleeve<Trade>> sleeveDocs, long ts) {
        return indexableBuilder().user("unit-tester").timestamp(ts)
           .records(sleeveToRecords(sleeveDocs)).build();
    }

    private static Iterable<IndexRecord> sleeveToRecords(Iterable<Sleeve<Trade>> sleeveDocs) {
        return transform(sleeveDocs, doc -> forApproval(doc, sl -> GSON.toJson(sl.source)));
    }

    private static List<Sleeve<Trade>> getMutations() {
        List<Sleeve<Trade>> mutations = Lists.newArrayList();
        mutations.addAll(createSleeves(ImmutableList.of(
              trade("/trades/021", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0),
              trade("/trades/022", "EMEA", "UK", "London", "IEU", "Andrew", 1420848000000L, 5.0)),
           newSleeveFunction(APPROVAL_INDEX, TRADE_TYPE)));
        List<IndexKey> updates = ImmutableList.of(keyFrom(APPROVAL_INDEX, TRADE_TYPE, "/trades/005", T2),
           keyFrom(APPROVAL_INDEX, TRADE_TYPE, "/trades/010", T2));
        mutations.add(Sleeve.create(updates.get(0),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0)));
        mutations.add(Sleeve.create(updates.get(1),
           trade("/trades/010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", 1425427200000L, 16.0)));
        List<IndexKey> deletes = ImmutableList.of(keyFrom(APPROVAL_INDEX, TRADE_TYPE, "/trades/004", T2));
        Iterables.addAll(mutations, transform(deletes, Sleeve::delete));
        return mutations;
    }
}
