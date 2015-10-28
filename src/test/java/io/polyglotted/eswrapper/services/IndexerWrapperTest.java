package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IgnoreErrors;
import io.polyglotted.eswrapper.indexing.IndexKey;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.query.QueryResponse;
import io.polyglotted.eswrapper.query.request.QueryBuilder;
import io.polyglotted.eswrapper.query.response.SimpleDoc;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.index.VersionType;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.eswrapper.indexing.IndexKey.keyWith;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.response.ResultBuilder.IndexKeyBuilder;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static io.polyglotted.eswrapper.services.Trade.tradesRequest;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class IndexerWrapperTest extends AbstractElasticTest {
    private static final String TRADES_INDEX = "trades_index";

    @Override
    protected void performSetup() {
        admin.dropIndex(TRADES_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), TRADES_INDEX);
    }

    @Test
    public void writeEmptyRequest() {
        indexer.index(new BulkRequest().refresh(true), IgnoreErrors.strict());
    }

    @Test
    public void writeIndexRequest() {
        Trade trade = trade("/trades/001", "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 20.0);
        long timestamp = 1425494500000L;

        IndexKey key = indexer.index(new IndexRequest(TRADES_INDEX, TRADE_TYPE, trade.address).opType(OpType.CREATE)
           .version(timestamp).versionType(VersionType.EXTERNAL).source(GSON.toJson(trade)));
        assertEquals(key, new IndexKey(TRADES_INDEX, TRADE_TYPE, trade.address, timestamp));

        SimpleDoc simpleDoc = query.get(keyWith(TRADES_INDEX, TRADE_TYPE, trade.address));
        assertEquals(simpleDoc.version(), timestamp);
        assertEquals(GSON.fromJson(GSON.toJson(simpleDoc.source), Trade.class), trade);
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

        QueryResponse origResponse = query.search(QueryBuilder.idRequest(new String[]{"/trades/001", "/trades/010"},
           ImmutableList.of(TRADE_TYPE), TRADES_INDEX), IndexKeyBuilder);
        assertEquals(transform(origResponse.resultsAs(IndexKey.class), key -> key.version), ImmutableList.of(2101L, 1101L));

        indexer.forceReindex(docs);
        admin.forceRefresh(TRADES_INDEX);

        QueryResponse queryResponse = query.search(QueryBuilder.idRequest(new String[]{"/trades/001", "/trades/010"},
           ImmutableList.of(TRADE_TYPE), TRADES_INDEX), IndexKeyBuilder);
        assertTrue(Iterables.all(queryResponse.resultsAs(IndexKey.class), key -> key.version == 1101L));
    }

    @Test
    public void deleteUpdatesNoop() {
        indexer.deleteUpdatesInHistory(TRADES_INDEX, ImmutableList.of(keyWith(TRADE_TYPE, "/trades/100"),
           keyWith(TRADE_TYPE, "/trades/101")));
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
