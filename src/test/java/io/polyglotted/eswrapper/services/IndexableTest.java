package io.polyglotted.eswrapper.services;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.*;
import io.polyglotted.eswrapper.query.request.Expression;
import io.polyglotted.eswrapper.query.response.SimpleDoc;
import io.polyglotted.eswrapper.validation.ValidException;
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
import static io.polyglotted.eswrapper.indexing.IndexKey.keyWith;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.SleeveDoc.createSleeves;
import static io.polyglotted.eswrapper.indexing.SleeveDoc.deleteSleeves;
import static io.polyglotted.eswrapper.indexing.SleeveDoc.newSleeve;
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
import static io.polyglotted.eswrapper.validation.Validity.validityBuilder;
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
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction()), T1));
        assertThat(fetchRecords(LIVE_INDEX).size(), is(20));
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(0));
    }

    @Test
    public void updateRecords() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction()), T1));

        List<SleeveDoc<Trade>> mutations = Lists.newArrayList();

        //creates
        mutations.addAll(createSleeves(ImmutableList.of(
           trade("/trades/021", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0),
           trade("/trades/022", "EMEA", "UK", "London", "IEU", "Andrew", 1420848000000L, 5.0)), newSleeveFunction()));

        List<IndexKey> updates = ImmutableList.of(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/005", T1),
           new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/010", T1));
        mutations.add(new SleeveDoc<>(updates.get(0),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0), true));
        mutations.add(new SleeveDoc<>(updates.get(1),
           trade("/trades/010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", 1425427200000L, 16.0), true));

        //deletes
        List<IndexKey> deletes = ImmutableList.of(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/019", T1));
        mutations.addAll(deleteSleeves(deletes));

        indexer.twoPhaseCommit(indexable(mutations, T2));

        assertThat(fetchRecords(LIVE_INDEX).size(), is(21));
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(3));
        assertHistory(updates, T1, "expired", T2);
        assertHistory(deletes, T1, "deleted", T2);
    }

    @Test
    public void deleteAndCreateAsNew() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction()), T1));

        List<IndexKey> deletes = ImmutableList.of(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/019", T1));
        indexer.twoPhaseCommit(indexable(deleteSleeves(deletes), T2));
        assertHistory(deletes, T1, "deleted", T2);

        List<Trade> newTrades = ImmutableList.of(
           trade("/trades/019", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0));
        indexer.twoPhaseCommit(indexable(createSleeves(newTrades, newSleeveFunction()), T2));
        assertThat(fetchRecords(LIVE_INDEX).size(), is(20));
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(1));
    }

    @Test
    public void createExistingRecordsShouldFail() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction()), T1));

        try {
            List<Trade> newTrades = ImmutableList.of(
               trade("/trades/020", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0));
            indexer.twoPhaseCommit(indexable(createSleeves(newTrades, newSleeveFunction()), T2));
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
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction()), T1));

        List<SleeveDoc<Trade>> update1 = ImmutableList.of(new SleeveDoc<>(
           new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/005", T1),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0), true));
        indexer.twoPhaseCommit(indexable(update1, T2));

        List<SleeveDoc<Trade>> update2 = ImmutableList.of(new SleeveDoc<>(
           new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/005", T1),
           trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 18.0), true));
        try {
            indexer.twoPhaseCommit(indexable(update2, T2));
            fail();

        } catch (IndexerException ie) {
            ImmutableMap<IndexKey, String> errorsMap = ie.errorsMap;
            assertThat(errorsMap.get(new IndexKey(INDEXABLE_INDEX, TRADE_TYPE, "/trades/005", T1)),
               startsWith("record not found"));
        }
        assertThat(fetchRecords(HISTORY_INDEX).size(), is(1));
    }

    @Test(expectedExceptions = ValidException.class)
    public void failedValidation() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction()), T1),
           currentDocs -> validityBuilder().memo("a", "induced fail").build());
    }

    private void assertHistory(Iterable<IndexKey> indexKeys, long version, String status, long expiry) {
        String[] ids = toArray(transform(indexKeys, IndexKey::uniqueId), String.class);
        Map<String, SimpleDoc> updatedItems = uniqueIndex(fetchRecords(INDEXABLE_INDEX, ids(ids)), doc -> doc.key().id);
        assertThat(updatedItems.size(), is(ids.length));
        for (String id : ids) {
            SimpleDoc doc = updatedItems.get(id);
            assertThat(doc.key().id, is(id));
            assertThat(doc.key().version, is(version));
            assertThat(doc.key.delete, is("deleted".equals(status)));
            assertThat(doc.strVal(STATUS_FIELD), is(status));
            assertThat(doc.longVal(EXPIRY_FIELD), is(expiry));
        }
    }

    private List<SimpleDoc> fetchRecords(String index, Expression... expressions) {
        return query.search(queryBuilder().index(index).size(100).expression(expressions).build(), null,
           SimpleDocBuilder).resultsAs(SimpleDoc.class);
    }

    private static Indexable indexable(Iterable<SleeveDoc<Trade>> sleeveDocs, long t1) {
        return indexableBuilder().index(INDEXABLE_INDEX).timestamp(t1)
           .records(sleeveToRecords(sleeveDocs))
           .build();
    }

    private static Iterable<IndexRecord> sleeveToRecords(Iterable<SleeveDoc<Trade>> sleeveDocs) {
        return transform(sleeveDocs, doc -> doc.toRecord(GSON::toJson));
    }

    private static Function<Trade, SleeveDoc<Trade>> newSleeveFunction() {
        return input -> newSleeve(input, (i) -> keyWith(TRADE_TYPE, i.address));
    }
}
