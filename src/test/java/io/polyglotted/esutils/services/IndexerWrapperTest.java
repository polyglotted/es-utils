package io.polyglotted.esutils.services;

import io.polyglotted.esutils.AbstractElasticTest;
import io.polyglotted.esutils.indexing.*;
import org.testng.annotations.Test;

import static io.polyglotted.esutils.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.esutils.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.esutils.services.Trade.trade;
import static io.polyglotted.esutils.services.Trade.tradesRequest;
import static java.util.Collections.singletonList;

public class IndexerWrapperTest extends AbstractElasticTest {
    private static final String TRADES_INDEX = "trades_index";

    @Override
    protected void performSetup() {
        admin.dropIndex(TRADES_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), TRADES_INDEX);
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
        indexer.index(tradesRequest(TRADES_INDEX, 1000L, singletonList(trade("/trades/001", "EMEA",
           "UK", "London", "IEU", "Alex", 1425427200000L, 20.0))), IgnoreErrors.from(false, true));
    }

    @Test(expectedExceptions = IndexerException.class)
    public void writeFailure() {
        admin.createType(typeBuilder().index(TRADES_INDEX).type(Trade.TRADE_TYPE)
           .fieldMapping(notAnalyzedStringField("hello")).strict(true).build());
        indexer.index(tradesRequest(TRADES_INDEX, 1000L, singletonList(trade("/trades/001", "EMEA",
           "UK", "London", "IEU", "Alex", 1425427200000L, 20.0))), IgnoreErrors.lenient());
    }
}
