package io.polyglotted.eswrapper.services;

import io.polyglotted.esmodel.api.SimpleDoc;
import io.polyglotted.esmodel.api.index.FieldType;
import io.polyglotted.esmodel.api.query.QueryResponse;
import io.polyglotted.esmodel.api.query.ResponseHeader;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.polyglotted.esmodel.api.Expressions.equalsTo;
import static io.polyglotted.esmodel.api.Expressions.textAnywhere;
import static io.polyglotted.esmodel.api.IndexKey.keyWith;
import static io.polyglotted.esmodel.api.index.FieldMapping.nestedField;
import static io.polyglotted.esmodel.api.index.FieldMapping.notAnalyzedField;
import static io.polyglotted.esmodel.api.index.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.esmodel.api.index.FieldMapping.simpleField;
import static io.polyglotted.esmodel.api.index.FieldType.STRING;
import static io.polyglotted.esmodel.api.query.Sort.sortAsc;
import static io.polyglotted.esmodel.api.query.StandardQuery.queryBuilder;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.QueryBuilder.queryToRequest;
import static io.polyglotted.eswrapper.query.ResultBuilder.NullBuilder;
import static io.polyglotted.eswrapper.query.ResultBuilder.SimpleDocBuilder;
import static io.polyglotted.eswrapper.query.ResultBuilder.SimpleObjectBuilder;
import static io.polyglotted.eswrapper.query.SourceBuilder.DEFAULT_BUILDER;
import static io.polyglotted.eswrapper.services.NamePath.NAMEPATH_TYPE;
import static io.polyglotted.eswrapper.services.NamePath.pathsRequest;
import static io.polyglotted.eswrapper.services.Nested.NESTED_TYPE;
import static io.polyglotted.eswrapper.services.Nested.nestedRequest;
import static io.polyglotted.eswrapper.services.Nested.nesteds;
import static io.polyglotted.eswrapper.services.SortableText.SORTABLE_TYPE;
import static io.polyglotted.eswrapper.services.SortableText.sortables;
import static io.polyglotted.eswrapper.services.SortableText.textsRequest;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.trade;
import static io.polyglotted.eswrapper.services.Trade.tradesRequest;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class QueryWrapperTest extends AbstractElasticTest {
    private static final String[] DUMMY_INDICES = {"dummy1", "dummy2"};

    @Override
    protected void performSetup() {
        admin.dropIndex(DUMMY_INDICES);
        admin.createIndex(IndexSetting.with(3, 0), emptyList(), DUMMY_INDICES);
    }

    @Test
    public void testIndexStatus() throws Exception {
        assertSampleValues(query.indexStatus(DUMMY_INDICES[0]));
        assertSampleValues(query.indexStatus(DUMMY_INDICES[1]));
        assertEquals(query.indexStatus("Foo").size(), 0);
    }

    private static void assertSampleValues(Map<String, String> settings) {
        assertEquals(settings.get("index.number_of_shards"), "3");
        assertEquals(settings.get("index.number_of_replicas"), "0");
        assertEquals(settings.get("index.analysis.analyzer.default.type"), "standard");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.filter.0"), "lowercase");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.tokenizer"), "whitespace");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.type"), "custom");
        assertEquals(settings.get("index.mapping.ignore_malformed"), "true");
    }

    @Test
    public void testGetDocsWithNoSource() {
        admin.createType(typeBuilder().index(DUMMY_INDICES[0]).type(TRADE_TYPE).storeSource(false)
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());
        indexer.index(tradesRequest(DUMMY_INDICES[0], System.currentTimeMillis()));
        QueryResponse response = query.search(queryBuilder().index(DUMMY_INDICES).size(20).build(),
           null, SimpleDocBuilder);
        List<SimpleDoc> simpleDocs = response.resultsAs(SimpleDoc.class);
        for (SimpleDoc doc : simpleDocs) assertEquals(doc.source.size(), 0);
    }

    @Test
    public void testPathHierarchy() {
        admin.createType(typeBuilder().index(DUMMY_INDICES[1]).type(NAMEPATH_TYPE).fieldMapping(
           notAnalyzedStringField("name")).fieldMapping(notAnalyzedStringField("path").isAPath()).build());
        indexer.index(pathsRequest(DUMMY_INDICES[1]));
        QueryResponse response = query.search(queryBuilder().index(DUMMY_INDICES).size(20)
           .expression(equalsTo("path.tree", "/users/aux")).build(), null, SimpleDocBuilder);
        List<SimpleDoc> simpleDocs = response.resultsAs(SimpleDoc.class);
        assertEquals(simpleDocs.size(), 2);
    }

    @Test
    public void testRawFieldQuery() {
        admin.createType(typeBuilder().index(DUMMY_INDICES[1]).type(SORTABLE_TYPE).fieldMapping(
           notAnalyzedStringField("name")).fieldMapping(simpleField("desc", STRING).addRawFields()).build());
        indexer.index(textsRequest(DUMMY_INDICES[1]));
        List<SortableText> unsorteds = query.search(queryBuilder().index(DUMMY_INDICES).sort(sortAsc("desc"))
           .build(), null, SimpleObjectBuilder(GSON, SortableText.class)).resultsAs(SortableText.class);
        List<SortableText> sorteds = query.search(queryBuilder().index(DUMMY_INDICES).sort(sortAsc("desc.raw"))
           .build(), null, SimpleObjectBuilder(GSON, SortableText.class)).resultsAs(SortableText.class);
        assertNotEquals(unsorteds, sorteds);
        assertEquals(sorteds, sortables());
    }

    @Test
    public void testCopyToField() {
        admin.createType(typeBuilder().index(DUMMY_INDICES[1]).type(NESTED_TYPE).allAnalyzer(null)
           .fieldMapping(notAnalyzedStringField("target").copyTo("freetext"))
           .fieldMapping(simpleField("freetext", STRING).analyzer("all_analyzer"))
           .fieldMapping(nestedField("child").property(singletonList(notAnalyzedField("effect", STRING)
              .copyTo("freetext")))).build());
        indexer.index(nestedRequest(DUMMY_INDICES[1]));

        List<Nested> nesteds = query.search(queryBuilder().index(DUMMY_INDICES).sort(sortAsc("target")).expression(
           textAnywhere("freetext", "proud.don")).build(), null, SimpleObjectBuilder(GSON, Nested.class))
           .resultsAs(Nested.class);
        assertEquals(nesteds, nesteds().subList(1, 3));
    }

    @Test
    public void testScroll() {
        indexer.index(tradesRequest(DUMMY_INDICES[0], System.currentTimeMillis()));
        QueryResponse queryResponse = query.search(queryToRequest(queryBuilder()
           .index(DUMMY_INDICES[0]).size(8).scrollTimeInMillis(3000L).build(), null), NullBuilder);
        queryResponse = query.scroll(queryResponse.nextScroll(), NullBuilder);
        assertEquals(queryResponse.header.totalHits, 20L);
        assertEquals(queryResponse.header.returnedHits, 8L);
        assertNotNull(queryResponse.header.scrollId);
    }

    @Test
    public void testSimpleScroll() {
        indexer.index(tradesRequest(DUMMY_INDICES[0], System.currentTimeMillis()));
        QueryResponse queryResponse = query.simpleScroll(queryToRequest(queryBuilder()
           .index(DUMMY_INDICES[0]).build(), null), NullBuilder);
        assertEquals(queryResponse.header, new ResponseHeader(0, 20, 20, null));
    }

    @Test
    public void testGetAs() {
        indexer.index(tradesRequest(DUMMY_INDICES[0], System.currentTimeMillis()));
        Map<String, ?> stringMap = query.getAs(keyWith(DUMMY_INDICES[0], TRADE_TYPE, "/trades/001"), DEFAULT_BUILDER);
        assertNotNull(stringMap);
    }

    @Test
    public void testGetAsTrade() {
        Trade trade = trade("/trades/001", "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 20.0);
        long timestamp = 1425494500000L;
        indexer.index(new IndexRequest(DUMMY_INDICES[0], TRADE_TYPE, trade.address).opType(IndexRequest.OpType.CREATE)
           .version(timestamp).versionType(VersionType.EXTERNAL).source(GSON.toJson(trade)));

        Trade actual = query.getAs(keyWith(DUMMY_INDICES[0], TRADE_TYPE, trade.address), Trade::tradeFromMap);
        assertEquals(actual, trade);
    }
}