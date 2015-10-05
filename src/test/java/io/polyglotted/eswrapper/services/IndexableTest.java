package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.*;
import io.polyglotted.eswrapper.query.request.Expression;
import io.polyglotted.eswrapper.query.response.SimpleDoc;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.polyglotted.eswrapper.indexing.Alias.aliasBuilder;
import static io.polyglotted.eswrapper.indexing.FieldMapping.EXPIRY_FIELD;
import static io.polyglotted.eswrapper.indexing.FieldMapping.STATUS_FIELD;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedField;
import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.updateRecord;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.StandardQuery.queryBuilder;
import static io.polyglotted.eswrapper.query.request.Expressions.archiveIndex;
import static io.polyglotted.eswrapper.query.request.Expressions.ids;
import static io.polyglotted.eswrapper.query.request.Expressions.liveIndex;
import static io.polyglotted.eswrapper.query.response.ResultBuilder.SimpleDocBuilder;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.sampleTrades;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.testng.FileAssert.fail;

public class IndexableTest extends AbstractElasticTest {
    private static final String INDEXABLE_INDEX = "indexable_index";
    private static final String LIVE_INDEX = "live_index";
    private static final String HISTORY_INDEX = "history_index";
    private static final long T1 = 1442784057000L;
    private static final long T2 = 1442784062000L;

    @Override
    protected void performSetup() {
        admin.dropIndex(INDEXABLE_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), INDEXABLE_INDEX);
        admin.createType(typeBuilder().index(INDEXABLE_INDEX).type(TRADE_TYPE)
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());
        admin.updateAliases(aliasBuilder().alias(LIVE_INDEX).index(INDEXABLE_INDEX).filter(liveIndex()).build(),
           aliasBuilder().alias(HISTORY_INDEX).index(INDEXABLE_INDEX).filter(archiveIndex()).build());
    }

    @Test
    public void indexNewRecords() {
        indexer.twoPhaseCommit(initialIndexable());
        assertThat(fetchRecords(LIVE_INDEX).size(), is(20));
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(0));
    }

    @Test
    public void updateRecords() {
        indexer.twoPhaseCommit(initialIndexable());

        IndexMutations<Trade> mutations = IndexMutations.<Trade>mutationsBuilder()
           .creates(ImmutableList.of(trade("/trades/021", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0),
              trade("/trades/022", "EMEA", "UK", "London", "IEU", "Andrew", 1420848000000L, 5.0)))

           .updates(ImmutableMap.of(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/005", T1),
              trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0),
              new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/010", T1),
              trade("/trades/010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", 1425427200000L, 16.0)))

           .deletes(ImmutableList.of(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/019", T1))).build();

        indexer.twoPhaseCommit(mutations.toIndexable(INDEXABLE_INDEX, T2,
           IndexableTest::toNewRecord, IndexableTest::toUpdateRecord));

        assertThat(fetchRecords(LIVE_INDEX).size(), is(21));
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(3));
        assertHistory(mutations.updates.keySet(), T1, "expired", T2);
        assertHistory(mutations.deletes, T1, "deleted", T2);
    }

    @Test
    public void deleteAndCreateAsNew() {
        indexer.twoPhaseCommit(initialIndexable());

        List<IndexKey> deleteTrades = ImmutableList.of(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/019", T1));
        indexer.twoPhaseCommit(indexableBuilder().index(INDEXABLE_INDEX).timestamp(T2)
           .records(transform(deleteTrades, IndexRecord::deleteRecord)).build());
        assertHistory(deleteTrades, T1, "deleted", T2);

        List<Trade> newTrades = ImmutableList.of(
           trade("/trades/019", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0));
        indexer.twoPhaseCommit(indexableBuilder().index(INDEXABLE_INDEX).timestamp(T2)
           .records(transform(newTrades, IndexableTest::toNewRecord)).build());
        assertThat(fetchRecords(LIVE_INDEX).size(), is(20));
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(1));
    }

    @Test
    public void createExistingRecordsShouldFail() {
        indexer.twoPhaseCommit(initialIndexable());

        try {
            List<Trade> newTrades = ImmutableList.of(
               trade("/trades/020", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0));
            indexer.twoPhaseCommit(indexableBuilder().index(INDEXABLE_INDEX).timestamp(T2)
               .records(transform(newTrades, IndexableTest::toNewRecord)).build());
            fail();

        } catch (IndexerException ie) {
            ImmutableMap<IndexKey, String> errorsMap = ie.errorsMap;
            assertThat(errorsMap.get(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/020", -1)),
               startsWith("DocumentAlreadyExistsException"));
        }
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(0));
    }

    @Test
    public void secondUpdateShouldFail() {
        indexer.twoPhaseCommit(initialIndexable());

        Map<IndexKey, Trade> update1 = ImmutableMap.of(
           new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/005", T1),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0));
        indexer.twoPhaseCommit(indexableBuilder().index(INDEXABLE_INDEX).timestamp(T2)
           .records(transform(update1.entrySet(), IndexableTest::toUpdateRecord)).build());

        Map<IndexKey, Trade> update2 = ImmutableMap.of(
           new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/005", T1),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 18.0));

        try {
            indexer.twoPhaseCommit(indexableBuilder().index(INDEXABLE_INDEX).timestamp(T2)
               .records(transform(update2.entrySet(), IndexableTest::toUpdateRecord)).build());
            fail();

        } catch (IndexerException ie) {
            ImmutableMap<IndexKey, String> errorsMap = ie.errorsMap;
            assertThat(errorsMap.get(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/005", T1)),
               startsWith("record not found"));
        }
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(1));
    }

    private void assertHistory(Iterable<IndexKey> indexKeys, long version, String status, long expiry) {
        String[] ids = toArray(transform(indexKeys, IndexKey::uniqueId), String.class);
        Map<String, SimpleDoc> updatedItems = uniqueIndex(fetchRecords(INDEXABLE_INDEX, ids(ids)), doc -> doc.key().id);
        assertThat(updatedItems.size(), is(ids.length));
        for (String id : ids) {
            SimpleDoc doc = updatedItems.get(id);
            assertThat(doc.key().id, is(id));
            assertThat(doc.key().version, is(version));
            assertThat(doc.strVal(STATUS_FIELD), is(status));
            assertThat(doc.longVal(EXPIRY_FIELD), is(expiry));
        }
    }

    private List<SimpleDoc> fetchRecords(String index, Expression... expressions) {
        return query.search(queryBuilder().index(index).size(100).expression(expressions).build(), null,
           SimpleDocBuilder).resultsAs(SimpleDoc.class);
    }

    private static Indexable initialIndexable() {
        return indexableBuilder().index(INDEXABLE_INDEX).timestamp(T1)
           .records(transform(sampleTrades(), IndexableTest::toNewRecord)).build();
    }

    private static IndexRecord toNewRecord(Trade trade) {
        return createRecord(TRADE_TYPE, trade.address).source(GSON.toJson(trade)).build();
    }

    private static IndexRecord toUpdateRecord(Map.Entry<IndexKey, Trade> entry) {
        return updateRecord(entry.getKey()).source(GSON.toJson(entry.getValue())).build();
    }
}
