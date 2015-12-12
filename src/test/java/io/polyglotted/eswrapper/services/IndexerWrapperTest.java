package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IgnoreErrors;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;
import io.polyglotted.pgmodel.search.query.QueryResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.VersionType;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.IndexSetting.settingBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.QueryBuilder.idRequest;
import static io.polyglotted.eswrapper.query.ResultBuilder.IndexKeyBuilder;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static io.polyglotted.eswrapper.services.Trade.tradeFromMap;
import static io.polyglotted.eswrapper.services.Trade.tradesRequest;
import static io.polyglotted.pgmodel.search.IndexKey.keyWith;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.pgmodel.util.ReflectionUtil.fieldValue;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class IndexerWrapperTest extends AbstractElasticTest {
    private static final String TRADES_INDEX = "trades_index";

    @Override
    protected void performSetup() {
        admin.dropIndex(TRADES_INDEX);
        admin.createIndex(settingBuilder(3, 0).any("index.ttl.interval", "1s").build(), TRADES_INDEX);
    }

    @Test
    public void writeEmptyRequest() {
        indexer.index(new BulkRequest().refresh(true), IgnoreErrors.strict());
    }

    @Test
    public void writeIndexRequest() {
        Trade trade = trade("/trades/001", "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 20.0);
        long t1 = 1425494500000L;
        indexer.index(new IndexRequest(TRADES_INDEX, TRADE_TYPE, trade.address).opType(OpType.CREATE)
           .version(t1).versionType(VersionType.EXTERNAL).source(GSON.toJson(trade)));
        checkSimpleTrade(trade, t1);

        long t2 = 1425494600000L;
        indexer.index(new UpdateRequest(TRADES_INDEX, TRADE_TYPE, trade.address)
           .version(t2).versionType(VersionType.FORCE).doc("trader", "Bob"));
        fieldValue(trade, "trader", "Bob");
        checkSimpleTrade(trade, t2);

        indexer.index(new DeleteRequest(TRADES_INDEX, TRADE_TYPE, trade.address));
        assertNull(query.get(keyWith(TRADES_INDEX, TRADE_TYPE, trade.address)));
    }

    private void checkSimpleTrade(Trade trade, long t1) {
        SimpleDoc simpleDoc = query.get(keyWith(TRADES_INDEX, TRADE_TYPE, trade.address));
        assertEquals((long) simpleDoc.version(), t1);
        assertEquals(tradeFromMap(simpleDoc.source), trade);
    }

    @Test
    public void writeAgainNoFailure() {
        indexer.index(tradesRequest(TRADES_INDEX, System.currentTimeMillis()), IgnoreErrors.strict());
        indexer.index(tradesRequest(TRADES_INDEX, System.currentTimeMillis(), singletonList(trade(
           "/trades/001", "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 20.0))), IgnoreErrors.lenient());
    }

    @Test
    public void writeOlderVersionNoFailure() {
        indexer.index(tradesRequest(TRADES_INDEX, 1100L));
        indexer.index(tradesRequest(TRADES_INDEX, 1000L, ImmutableList.of(trade("/trades/001", "EMEA",
           "UK", "London", "IEU", "Alex", 1425427200000L, 20.0), trade("/trades/021", "APAC",
           "SG", "Singapore", "IEU", "Alex", 1425427200000L, 27.0))), IgnoreErrors.from(false, true));
    }

    @Test(expectedExceptions = IndexerException.class)
    public void writeFailure() {
        admin.createType(typeBuilder().index(TRADES_INDEX).type(Trade.TRADE_TYPE)
           .fieldMapping(notAnalyzedStringField("hello")).strict(true).build());
        indexer.index(tradesRequest(TRADES_INDEX, 1000L, singletonList(trade("/trades/001", "EMEA",
           "UK", "London", "IEU", "Alex", 1425427200000L, 20.0))), IgnoreErrors.lenient());
    }

    @Test
    public void forceReindex() {
        indexer.index(tradesRequest(TRADES_INDEX, 1101L));
        List<SimpleDoc> docs = indexer.getCurrent(TRADES_INDEX, "/trades/001", "/trades/010");

        indexer.index(new BulkRequest().refresh(true).add(new IndexRequest(TRADES_INDEX, TRADE_TYPE,
           "/trades/001").version(2101L).versionType(VersionType.EXTERNAL).source(GSON.toJson(trade("/trades/001",
           "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 20.0)))));

        QueryResponse origResponse = query.search(idRequest(new String[]{"/trades/001", "/trades/010"},
           ImmutableList.of(TRADE_TYPE), TRADES_INDEX), IndexKeyBuilder);
        assertEquals(transform(origResponse.resultsAs(IndexKey.class), key -> key.version), ImmutableList.of(2101L, 1101L));

        indexer.forceReindex(docs);
        admin.forceRefresh(TRADES_INDEX);

        QueryResponse queryResponse = query.search(idRequest(new String[]{"/trades/001", "/trades/010"},
           ImmutableList.of(TRADE_TYPE), TRADES_INDEX), IndexKeyBuilder);
        assertTrue(Iterables.all(queryResponse.resultsAs(IndexKey.class), key -> key.version == 1101L));
    }

    @Test
    public void deleteUpdatesNoop() {
        indexer.deleteUpdatesInHistory(TRADES_INDEX, ImmutableList.of(keyWith(TRADES_INDEX, TRADE_TYPE, "/trades/100"),
           keyWith(TRADES_INDEX, TRADE_TYPE, "/trades/101")));
        admin.forceRefresh(TRADES_INDEX);
    }

    @Test
    public void lockFailed() {
        indexer.unlockIndex(TRADES_INDEX);
        indexer.lockTheIndexOrFail(TRADES_INDEX);
        try {
            indexer.lockTheIndexOrFail(TRADES_INDEX);
            fail();

        } catch (IllegalStateException ise) {
            indexer.unlockIndex(TRADES_INDEX);
        }
        admin.forceRefresh(TRADES_INDEX);
    }
}
