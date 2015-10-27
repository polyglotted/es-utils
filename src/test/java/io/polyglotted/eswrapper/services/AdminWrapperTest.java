package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.polyglotted.eswrapper.indexing.Alias.aliasBuilder;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.eswrapper.indexing.IndexKey.keyWith;
import static io.polyglotted.eswrapper.indexing.IndexSetting.settingBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.request.Expressions.in;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertEquals;

public class AdminWrapperTest extends AbstractElasticTest {

    private static final String[] ADMIN_INDICES = new String[]{"admin.test.live", "admin.test.history"};
    private static final String ADMIN_ALIAS = "admin.test";
    private static final String ADMIN_TYPE = "AdminType";

    @Override
    protected void performSetup() {
        admin.dropIndex(ADMIN_INDICES);
    }

    @Test
    public void createTypeAcrossIndices() {
        admin.waitForYellowStatus();
        admin.createIndex(IndexSetting.with(3, 1), singletonList(ADMIN_ALIAS), ADMIN_INDICES);
        admin.createType(typeBuilder().index(ADMIN_ALIAS).type(ADMIN_TYPE)
           .fieldMapping(notAnalyzedStringField("a")).build());

        String MAPPING = "{\"AdminType\":{\"_all\":{\"enabled\":true,\"analyzer\":\"all_analyzer\"},\"properties\":" +
           "{\"&bytes\":{\"type\":\"binary\"},\"&expiry\":{\"type\":\"long\",\"include_in_all\":false},\"&status\":" +
           "{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true,\"include_in_all\":false},\"a\":" +
           "{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true}}}}";
        assertThat(admin.getMapping(ADMIN_INDICES[0], ADMIN_TYPE), is(MAPPING));
        assertThat(admin.getMapping(ADMIN_INDICES[1], ADMIN_TYPE), is(MAPPING));
        assertThat(admin.getMapping(ADMIN_ALIAS, ADMIN_TYPE), is(MAPPING));

        //test no-op actions
        admin.createIndex(IndexSetting.with(5, 2), ADMIN_INDICES[0]);
        admin.createType(typeBuilder().index(ADMIN_ALIAS).type(ADMIN_TYPE)
           .fieldMapping(notAnalyzedStringField("b")).build());

        admin.dropIndex(ADMIN_ALIAS);

        assertThat(admin.indexExists(ADMIN_INDICES), is(false));
    }

    @Test
    public void updateAlias() {
        String[] NEW_INDICES = new String[]{"new.test.live", "new.test.history"};
        admin.createIndex(IndexSetting.with(3, 1), singletonList(ADMIN_ALIAS), ADMIN_INDICES);
        admin.createIndex(IndexSetting.with(3, 1), NEW_INDICES);
        admin.createType(typeBuilder().index(NEW_INDICES[0]).type(ADMIN_TYPE)
           .fieldMapping(notAnalyzedStringField("a")).build());
        admin.createType(typeBuilder().index(NEW_INDICES[1]).type(ADMIN_TYPE)
           .fieldMapping(notAnalyzedStringField("a")).build());

        admin.updateAliases(aliasBuilder().alias(ADMIN_ALIAS).index(ADMIN_INDICES).remove().build(),
           aliasBuilder().alias(ADMIN_ALIAS).filter(in("a", "aa")).index(NEW_INDICES).build());

        admin.dropIndex(ADMIN_ALIAS);

        assertThat(admin.indexExists(NEW_INDICES), is(false));
        assertThat(admin.indexExists(ADMIN_INDICES), is(true));
    }

    @Test
    public void updateSetting() {
        admin.createIndex(IndexSetting.with(3, 1), singletonList(ADMIN_ALIAS), ADMIN_INDICES);
        Map<String, Map<String, String>> originalMap = query.indexStatus(ADMIN_INDICES);
        assertSettings(originalMap.get(ADMIN_INDICES[0]), "1", null);
        assertSettings(originalMap.get(ADMIN_INDICES[1]), "1", null);

        admin.updateSetting(settingBuilder().numberOfReplicas(2).refreshInterval(-1).build(), ADMIN_ALIAS);
        Map<String, Map<String, String>> updatedMap = query.indexStatus(ADMIN_INDICES);
        assertSettings(updatedMap.get(ADMIN_INDICES[0]), "2", "-1");
        assertSettings(updatedMap.get(ADMIN_INDICES[1]), "2", "-1");
    }

    private static void assertSettings(Map<String, String> settings, String replicas, String interval) {
        assertEquals(settings.get("index.number_of_shards"), "3");
        assertEquals(settings.get("index.number_of_replicas"), replicas);
        assertEquals(settings.get("index.refresh_interval"), interval);
        assertEquals(settings.get("index.analysis.analyzer.default.type"), "standard");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.type"), "custom");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.tokenizer"), "whitespace");
        assertEquals(settings.get("index.analysis.analyzer.all_analyzer.filter.0"), "lowercase");
        assertEquals(settings.get("index.analysis.analyzer.path_analyzer.tokenizer"), "path_hierarchy");
        assertEquals(settings.get("index.analysis.analyzer.path_analyzer.filter.0"), "lowercase");
    }

    @Test
    public void generateSequence() {
        admin.createIndex(settingBuilder().numberOfShards(1).autoExpandReplicas().build(), ADMIN_INDICES[0]);
        admin.createForcedType(ADMIN_INDICES[0], ADMIN_TYPE);
        for(long counter=0; counter<10; counter++) {
            long sequence = indexer.generateSequence(keyWith(ADMIN_INDICES[0], ADMIN_TYPE, "Sequence"));
            assertEquals(sequence, counter+1);
        }
    }

    @Test
    public void generateBlockSequences() {
        admin.createIndex(settingBuilder().numberOfShards(1).autoExpandReplicas().build(), ADMIN_INDICES[0]);
        admin.createForcedType(ADMIN_INDICES[0], ADMIN_TYPE);
        List<Long> sequences = indexer.generateSequences(keyWith(ADMIN_INDICES[0], ADMIN_TYPE, "Sequence"), 10);
        assertEquals(sequences, ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
    }
}
