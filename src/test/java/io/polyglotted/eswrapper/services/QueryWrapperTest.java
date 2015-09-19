package io.polyglotted.eswrapper.services;

import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class QueryWrapperTest extends AbstractElasticTest {
    private static final String[] DUMMY_INDICES = { "dummy1", "dummy2" };

    @Override
    protected void performSetup() {
        admin.dropIndex(DUMMY_INDICES);
        admin.createIndex(IndexSetting.with(3, 0), DUMMY_INDICES);
    }

    @Test
    public void testIndexStatus() throws Exception {
        Map<String, Map<String, String>> settingsMap = query.indexStatus(DUMMY_INDICES);
        assertSampleValues(settingsMap.get(DUMMY_INDICES[0]));
        assertSampleValues(settingsMap.get(DUMMY_INDICES[1]));
    }

    private static void assertSampleValues(Map<String, String> settings) {
        assertEquals(settings.get("index.number_of_shards"), "3");
        assertEquals(settings.get("index.number_of_replicas"), "0");
        assertEquals(settings.get("index.analysis.analyzer.default.type"), "keyword");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.filter.0"), "lowercase");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.tokenizer"), "whitespace");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.type"), "custom");
        assertEquals(settings.get("index.mapping.ignore_malformed"), "true");
    }
}