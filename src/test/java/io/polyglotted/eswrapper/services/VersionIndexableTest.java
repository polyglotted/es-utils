package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.FieldType;
import io.polyglotted.eswrapper.indexing.IndexKey;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.query.StandardResponse;
import io.polyglotted.eswrapper.query.request.QueryBuilder;
import io.polyglotted.eswrapper.query.response.SimpleDoc;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedField;
import static io.polyglotted.eswrapper.indexing.IndexKey.keyWith;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.response.ResultBuilder.IndexKeyBuilder;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static io.polyglotted.eswrapper.services.Trade.tradesRequest;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class VersionIndexableTest extends AbstractElasticTest {
    private static final String LIVE_INDEX = "live_index";
    private static final String HISTORY_INDEX = "history_index";
    private static final String ALL_INDICES = "all_indices";

    @Override
    protected void performSetup() {
        admin.dropIndex(LIVE_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), ImmutableList.of(ALL_INDICES), LIVE_INDEX, HISTORY_INDEX);
        admin.createType(typeBuilder().index(ALL_INDICES).type(TRADE_TYPE)
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());
    }

    @Test
    public void forceReindex() {
        indexer.index(tradesRequest(LIVE_INDEX, 1101L));
        List<SimpleDoc> docs = indexer.getCurrent(LIVE_INDEX, "/trades/001", "/trades/010");

        indexer.index(new BulkRequest().refresh(true).add(new IndexRequest(LIVE_INDEX, TRADE_TYPE,
           "/trades/001").version(2101L).versionType(VersionType.EXTERNAL).source(GSON.toJson(trade("/trades/001",
           "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 20.0)))));

        StandardResponse origResponse = query.search(QueryBuilder.idRequest(new String[]{"/trades/001", "/trades/010"},
           ImmutableList.of(TRADE_TYPE), LIVE_INDEX), IndexKeyBuilder);
        assertEquals(transform(origResponse.resultsAs(IndexKey.class), key -> key.version), ImmutableList.of(2101L, 1101L));

        indexer.forceReindex(docs);
        admin.forceRefresh(LIVE_INDEX);

        StandardResponse standardResponse = query.search(QueryBuilder.idRequest(new String[]{"/trades/001", "/trades/010"},
           ImmutableList.of(TRADE_TYPE), LIVE_INDEX), IndexKeyBuilder);
        assertTrue(Iterables.all(standardResponse.resultsAs(IndexKey.class), key -> key.version == 1101L));
    }

    @Test
    public void deleteUpdatesNoop() {
        indexer.deleteUpdatesInHistory(HISTORY_INDEX, ImmutableList.of(keyWith(TRADE_TYPE, "/trades/100"),
           keyWith(TRADE_TYPE, "/trades/101")));
        admin.forceRefresh(HISTORY_INDEX);
    }

    @Test
    public void lockFailed() {
        indexer.unlockIndex(LIVE_INDEX);
        indexer.lockTheIndexOrFail(LIVE_INDEX);
        try {
            indexer.lockTheIndexOrFail(LIVE_INDEX);
            fail();

        } catch (IllegalStateException ise) {
            indexer.unlockIndex(LIVE_INDEX);
        }
        admin.forceRefresh(LIVE_INDEX);
    }
}
