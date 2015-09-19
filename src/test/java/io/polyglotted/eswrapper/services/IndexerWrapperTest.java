package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.testng.annotations.Test;

import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static io.polyglotted.eswrapper.services.Trade.tradesRequest;
import static java.util.Collections.singletonList;

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
}