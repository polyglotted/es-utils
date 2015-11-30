package io.polyglotted.eswrapper.services;

import io.polyglotted.esmodel.api.SimpleDoc;
import io.polyglotted.esmodel.api.query.QueryResponse;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.Indexable;
import org.testng.annotations.Test;

import java.util.List;

import static io.polyglotted.esmodel.api.Expressions.equalsTo;
import static io.polyglotted.esmodel.api.Expressions.hasChild;
import static io.polyglotted.esmodel.api.Expressions.hasParent;
import static io.polyglotted.esmodel.api.Expressions.in;
import static io.polyglotted.esmodel.api.IndexKey.keyWithParent;
import static io.polyglotted.esmodel.api.index.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.esmodel.api.query.StandardQuery.queryBuilder;
import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.ResultBuilder.SimpleDocBuilder;
import static io.polyglotted.eswrapper.query.ResultBuilder.SimpleObjectBuilder;
import static io.polyglotted.eswrapper.services.Portfolio.FieldAddress;
import static io.polyglotted.eswrapper.services.Portfolio.PORTFOLIO_TYPE;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ParentChildTest extends AbstractElasticTest {
    private static final String PC_INDEX = "parent_child_index";

    @Override
    protected void performSetup() {
        admin.dropIndex(PC_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), PC_INDEX);
        admin.createType(typeBuilder().index(PC_INDEX).type(PORTFOLIO_TYPE)
           .fieldMapping(notAnalyzedStringField(FieldAddress)).build());
        admin.createType(typeBuilder().index(PC_INDEX).type(TRADE_TYPE).parent(PORTFOLIO_TYPE)
           .fieldMapping(notAnalyzedStringField(FieldAddress)).build());
    }

    @Test
    public void writeParentChildRequest() {
        long timestamp = 1425494500000L;
        Portfolio portfolio = new Portfolio("/portfolios/1st", "first portfolio");
        Trade trade = trade("/trades/001", "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 20.0);
        Indexable indexable = indexableBuilder().timestamp(timestamp).index(PC_INDEX).records(asList(
           createRecord(PORTFOLIO_TYPE, portfolio.address).source(GSON.toJson(portfolio)).build(),
           createRecord(keyWithParent("", TRADE_TYPE, trade.address, portfolio.address)).source(GSON.toJson(trade)).build()
        )).build();
        indexer.twoPhaseCommit(indexable);

        ensureHasParent(portfolio, trade);
        ensureHasChild(portfolio, trade);
        ensureBothDocs(portfolio, trade);
    }

    private void ensureHasParent(Portfolio portfolio, Trade trade) {
        QueryResponse response = query.search(queryBuilder().index(PC_INDEX)
              .expression(hasParent(PORTFOLIO_TYPE, equalsTo(FieldAddress, portfolio.address))).build(),
           null, SimpleObjectBuilder(GSON, Trade.class));
        List<Trade> simpleDocs = response.resultsAs(Trade.class);
        assertThat(simpleDocs.size(), is(1));
        assertThat(simpleDocs.get(0), is(trade));
    }

    private void ensureHasChild(Portfolio portfolio, Trade trade) {
        QueryResponse response = query.search(queryBuilder().index(PC_INDEX)
              .expression(hasChild(TRADE_TYPE, equalsTo(FieldAddress, trade.address))).build(),
           null, SimpleObjectBuilder(GSON, Portfolio.class));
        List<Portfolio> simpleDocs = response.resultsAs(Portfolio.class);
        assertThat(simpleDocs.size(), is(1));
        assertThat(simpleDocs.get(0), is(portfolio));
    }

    private void ensureBothDocs(Portfolio portfolio, Trade trade) {
        QueryResponse response = query.search(queryBuilder().index(PC_INDEX)
              .expression(in(FieldAddress, portfolio.address, trade.address)).build(),
           null, SimpleDocBuilder);
        List<SimpleDoc> simpleDocs = response.resultsAs(SimpleDoc.class);
        assertThat(simpleDocs.size(), is(2));
    }
}
