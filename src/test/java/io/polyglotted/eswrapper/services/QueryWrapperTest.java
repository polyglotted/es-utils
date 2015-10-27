package io.polyglotted.eswrapper.services;

import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.FieldType;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.query.QueryResponse;
import io.polyglotted.eswrapper.query.response.ResponseHeader;
import io.polyglotted.eswrapper.query.response.SimpleDoc;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedField;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.StandardQuery.queryBuilder;
import static io.polyglotted.eswrapper.query.request.Expressions.equalsTo;
import static io.polyglotted.eswrapper.query.request.QueryBuilder.queryToRequest;
import static io.polyglotted.eswrapper.query.response.ResultBuilder.NullBuilder;
import static io.polyglotted.eswrapper.query.response.ResultBuilder.SimpleDocBuilder;
import static io.polyglotted.eswrapper.services.NamePath.NAMEPATH_TYPE;
import static io.polyglotted.eswrapper.services.NamePath.pathsRequest;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.tradesRequest;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class QueryWrapperTest extends AbstractElasticTest {
    private static final String[] DUMMY_INDICES = {"dummy1", "dummy2"};

    @Override
    protected void performSetup() {
        admin.dropIndex(DUMMY_INDICES);
        admin.createIndex(IndexSetting.with(3, 0), DUMMY_INDICES);
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
           notAnalyzedStringField("name")).fieldMapping(notAnalyzedStringField("path").isAPath(true)).build());
        indexer.index(pathsRequest(DUMMY_INDICES[1]));
        QueryResponse response = query.search(queryBuilder().index(DUMMY_INDICES).size(20)
           .expression(equalsTo("path.tree", "/users/aux")).build(), null, SimpleDocBuilder);
        List<SimpleDoc> simpleDocs = response.resultsAs(SimpleDoc.class);
        assertEquals(simpleDocs.size(), 2);
    }

    @Test
    public void testScroll() {
        indexer.index(tradesRequest(DUMMY_INDICES[0], System.currentTimeMillis()));
        QueryResponse queryResponse = query.search(queryToRequest(queryBuilder()
           .index(DUMMY_INDICES[0]).size(8).scrollTimeInMillis(3000L).build(), null), NullBuilder);
        queryResponse = query.scroll(queryResponse.nextScroll(), NullBuilder);
        assertEquals(queryResponse.header.hits, 20L);
        assertEquals(queryResponse.header.returned, 8L);
        assertNotNull(queryResponse.header.id);
    }

    @Test
    public void testSimpleScroll() {
        indexer.index(tradesRequest(DUMMY_INDICES[0], System.currentTimeMillis()));
        QueryResponse queryResponse = query.simpleScroll(queryToRequest(queryBuilder()
           .index(DUMMY_INDICES[0]).build(), null), NullBuilder);
        assertEquals(queryResponse.header, new ResponseHeader(0, 20, 20, null));
    }
}