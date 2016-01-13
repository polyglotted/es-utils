package io.polyglotted.eswrapper.services;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
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
import java.util.Map;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.fromSleeve;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.ResultBuilder.SimpleDocBuilder;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.sampleTrades;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static io.polyglotted.pgmodel.search.DocStatus.DELETED;
import static io.polyglotted.pgmodel.search.DocStatus.EXPIRED;
import static io.polyglotted.pgmodel.search.IndexKey.keyFrom;
import static io.polyglotted.pgmodel.search.IndexKey.keyWith;
import static io.polyglotted.pgmodel.search.Sleeve.createSleeves;
import static io.polyglotted.pgmodel.search.Sleeve.newSleeve;
import static io.polyglotted.pgmodel.search.index.Alias.aliasBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedField;
import static io.polyglotted.pgmodel.search.index.HiddenFields.EXPIRY_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.STATUS_FIELD;
import static io.polyglotted.pgmodel.search.index.HiddenFields.UPDATER_FIELD;
import static io.polyglotted.pgmodel.search.query.Expressions.archiveIndex;
import static io.polyglotted.pgmodel.search.query.Expressions.ids;
import static io.polyglotted.pgmodel.search.query.Expressions.liveIndex;
import static io.polyglotted.pgmodel.search.query.StandardQuery.queryBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.testng.FileAssert.fail;

public class IndexableTest extends AbstractElasticTest {
    private static final String INDEXABLE_INDEX = "indexable_index";
    private static final String LIVE_ALIAS = "indexable_index.live";
    private static final String ARCHIVE_ALIAS = "indexable_index.archive";
    private static final long T1 = 1442784057000L;
    private static final long T2 = 1442784062000L;

    @Override
    protected void performSetup() {
        admin.dropIndex(INDEXABLE_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), INDEXABLE_INDEX);
        admin.createType(typeBuilder().index(INDEXABLE_INDEX).type(TRADE_TYPE)
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());
        admin.updateAliases(aliasBuilder().alias(LIVE_ALIAS).index(INDEXABLE_INDEX).filter(liveIndex()).build(),
           aliasBuilder().alias(ARCHIVE_ALIAS).index(INDEXABLE_INDEX).filter(archiveIndex()).build());
    }

    @Test
    public void indexNewRecords() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)), T1));
        assertThat(fetchRecords(query, LIVE_ALIAS).size(), is(20));
        assertThat(fetchRecords(query, ARCHIVE_ALIAS).size(), is(0));
    }

    @Test
    public void updateRecords() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)), T1));

        List<Sleeve<Trade>> mutations = Lists.newArrayList();
        //creates
        mutations.addAll(createSleeves(of(
              trade("trades:021", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0),
              trade("trades:022", "EMEA", "UK", "London", "IEU", "Andrew", 1420848000000L, 5.0)),
           newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)));

        List<IndexKey> updates = of(keyFrom(LIVE_ALIAS, TRADE_TYPE, "trades:005", T1),
           keyFrom(LIVE_ALIAS, TRADE_TYPE, "trades:010", T1));
        mutations.add(Sleeve.create(updates.get(0),
           trade("trades:005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0)));
        mutations.add(Sleeve.create(updates.get(1),
           trade("trades:010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", 1425427200000L, 16.0)));

        //deletes
        List<IndexKey> deletes = of(keyFrom(LIVE_ALIAS, TRADE_TYPE, "trades:019", T1));
        Iterables.addAll(mutations, transform(deletes, Sleeve::delete));

        indexer.twoPhaseCommit(indexable(mutations, T2));

        assertThat(fetchRecords(query, LIVE_ALIAS).size(), is(21));
        assertThat(fetchRecords(query, ARCHIVE_ALIAS).size(), is(3));
        assertHistory(updates, T1, EXPIRED.name(), T2);
        assertHistory(deletes, T1, DELETED.name(), T2);
    }

    @Test
    public void deleteAndCreateAsNew() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)), T1));

        List<IndexKey> deletes = of(keyFrom(LIVE_ALIAS, TRADE_TYPE, "trades:019", T1));
        indexer.twoPhaseCommit(indexable(transform(deletes, Sleeve::delete), T2));
        assertHistory(deletes, T1, DELETED.name(), T2);

        List<Trade> newTrades = of(trade("trades:019", "EMEA", "UK", "London", "IEU", "Andrew", 1425427200000L, 40.0));
        indexer.twoPhaseCommit(indexable(createSleeves(newTrades, newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)), T2));
        assertThat(fetchRecords(query, LIVE_ALIAS).size(), is(20));
        assertThat(fetchRecords(query, ARCHIVE_ALIAS).size(), is(1));
    }

    @Test
    public void createExistingRecordsShouldFail() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)), T1));

        List<Sleeve<Trade>> newTrades = createSleeves(of(trade("trades:020", "EMEA", "UK", "London",
           "IEU", "Andrew", 1425427200000L, 40.0)), newSleeveFunction(LIVE_ALIAS, TRADE_TYPE));
        try {
            indexer.twoPhaseCommit(indexable(newTrades, T2));
            fail();

        } catch (IndexerException ie) {
            Map<IndexKey, String> errorsMap = checkAssertValidity(ie);
            assertThat(errorsMap.get(newTrades.get(0).key), equalTo("record already exists"));
        }
        assertThat(fetchRecords(query, ARCHIVE_ALIAS).size(), is(0));
    }

    @Test
    public void secondUpdateShouldFail() {
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)), T1));

        List<Sleeve<Trade>> update1 = of(Sleeve.create(keyFrom(LIVE_ALIAS, TRADE_TYPE, "trades:005", T1),
           trade("trades:005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0)));
        indexer.twoPhaseCommit(indexable(update1, T2));

        List<Sleeve<Trade>> update2 = of(Sleeve.create(keyFrom(LIVE_ALIAS, TRADE_TYPE, "trades:005", T1),
           trade("trades:005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 18.0)));
        try {
            indexer.twoPhaseCommit(indexable(update2, T2));
            fail();

        } catch (IndexerException ie) {
            Map<IndexKey, String> errorsMap = checkAssertValidity(ie);
            assertThat(errorsMap.get(update2.get(0).key), equalTo("version conflict for update"));
        }
        assertThat(fetchRecords(query, ARCHIVE_ALIAS).size(), is(1));
    }

    @Test
    public void updateWithoutCreateShouldFail() {
        List<Sleeve<Trade>> update1 = of(Sleeve.create(keyFrom(LIVE_ALIAS, TRADE_TYPE, "trades:005", T1),
           trade("trades:005", "EMEA", "UK", "London", "LME", "Chandler", 1425427200000L, 30.0)));
        try {
            indexer.twoPhaseCommit(indexable(update1, T2));
            fail();

        } catch (IndexerException ie) {
            Map<IndexKey, String> errorsMap = checkAssertValidity(ie);
            assertThat(errorsMap.get(update1.get(0).key), equalTo("record not found for update"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void inducedFailureTest() {
        IndexKey indexKey = keyWith(INDEXABLE_INDEX, "MyTrade", "trades:005");
        try {
            admin.createType(typeBuilder().index(LIVE_ALIAS).strict(true).type("MyTrade")
               .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());

            Indexable indexable = indexableBuilder().timestamp(T1).user("unit-tester").records(createRecord(
               indexKey, "{\"address\": \"trades:005\",\"tradeDate\": \"fail dude\",\"value\": 30}")).build();
            indexer.twoPhaseCommit(indexable);
            fail();

        } catch (IndexerException ie) {
            Map<IndexKey, String> errorsMap = checkAssertValidity(ie);
            assertThat(errorsMap.get(indexKey), startsWith("StrictDynamicMappingException"));
        }
    }

    @Test
    public void failedValidation() {
        ImmutableMap<String, String> errors = ImmutableMap.of("a", "induced validation fail");
        try {
            indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(),
               newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)), T1), (keys, docs) -> errors);
            fail("cannot pass valid");

        } catch (IndexerException ie) {
            assertThat(checkAssertValidity(ie), is(errors));
        }
        indexer.twoPhaseCommit(indexable(createSleeves(sampleTrades(), newSleeveFunction(LIVE_ALIAS, TRADE_TYPE)), T2));
    }

    private void assertHistory(Iterable<IndexKey> indexKeys, long version, String status, long expiry) {
        String[] ids = toArray(transform(indexKeys, IndexKey::uniqueId), String.class);
        Map<String, SimpleDoc> updatedItems = uniqueIndex(fetchRecords(query, ARCHIVE_ALIAS, ids(ids)), SimpleDoc::id);
        assertThat(updatedItems.size(), is(ids.length));
        for (String id : ids) {
            SimpleDoc doc = updatedItems.get(id);
            assertThat(doc.key().id, is(id));
            assertThat(doc.key().version, is(version));
            assertThat(doc.strVal(STATUS_FIELD), is(status));
            assertThat(doc.longStrVal(EXPIRY_FIELD), is(expiry));
            assertThat(doc.strVal(UPDATER_FIELD), is("unit-tester"));
        }
    }

    private static Indexable indexable(Iterable<Sleeve<Trade>> sleeveDocs, long t1) {
        return indexableBuilder().user("unit-tester").timestamp(t1).records(sleeveToRecords(sleeveDocs)).build();
    }

    private static Iterable<IndexRecord> sleeveToRecords(Iterable<Sleeve<Trade>> sleeveDocs) {
        return transform(sleeveDocs, doc -> fromSleeve(doc, sl -> GSON.toJson(sl.source)));
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> checkAssertValidity(IndexerException ie) {
        assertThat(ie.getMessage(), is(ie.errorsMap.get("ERROR_MESSAGE")));
        return (Map<K, V>) ie.errorsMap.get("ERRORS");
    }

    static List<SimpleDoc> fetchRecords(QueryWrapper query, String index, Expression... expressions) {
        return query.search(queryBuilder().index(index).size(100).expression(expressions).build(), null,
           SimpleDocBuilder).resultsAs(SimpleDoc.class);
    }

    static Function<Trade, Sleeve<Trade>> newSleeveFunction(String index, String type) {
        return input -> newSleeve(input, (i) -> keyWith(index, type, i.address));
    }
}
