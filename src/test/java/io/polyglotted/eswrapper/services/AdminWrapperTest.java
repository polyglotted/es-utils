package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.TypeMapping;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.uniqueIndex;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.deserList;
import static io.polyglotted.eswrapper.indexing.IndexSetting.settingBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.pgmodel.search.IndexKey.keyWith;
import static io.polyglotted.pgmodel.search.index.Alias.aliasBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.pgmodel.search.query.Expressions.in;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

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

        TypeMapping mapping = typeBuilder().index(ADMIN_ALIAS).type(ADMIN_TYPE)
           .fieldMapping(notAnalyzedStringField("a")).build();
        admin.createType(mapping);
        assertThat(admin.getMapping(ADMIN_ALIAS, ADMIN_TYPE), admin.getMapping(ADMIN_ALIAS, ADMIN_TYPE),
           is(mapping.mappingJson()));
        assertThat(admin.getMapping(ADMIN_INDICES[0], ADMIN_TYPE), is(mapping.mappingJson()));
        assertThat(admin.getMapping(ADMIN_INDICES[1], ADMIN_TYPE), is(mapping.mappingJson()));

        //test no-op actions
        admin.createIndex(IndexSetting.with(5, 2), ADMIN_INDICES[0]);
        admin.createType(typeBuilder().index(ADMIN_ALIAS).type(ADMIN_TYPE)
           .fieldMapping(notAnalyzedStringField("b")).build());

        admin.dropType(ADMIN_ALIAS, ADMIN_TYPE);
        assertFalse(admin.typeExists(ADMIN_ALIAS, ADMIN_TYPE));

        admin.dropIndex(ADMIN_ALIAS);

        assertThat(admin.indexExists(ADMIN_INDICES), is(false));
    }

    @Test
    public void validateGetIndices() {
        admin.createIndex(IndexSetting.with(3, 0), ImmutableList.of(), "first", "second", "third", "fourth");
        admin.updateAliases(aliasBuilder().alias("TestAlias").index("second", "fourth").build());

        assertIndexData(new String[]{"first", "second", "third", "fourth"});
        assertIndexData(new String[]{"fourth"}, "fourth");
        assertIndexData(new String[]{"first", "second"}, "first", "second");
        assertIndexData(new String[]{"second", "fourth"}, "TestAlias");

        admin.dropIndex("first", "second", "third", "fourth");
    }

    private void assertIndexData(String[] expected, String... originals) {
        String data = admin.getIndex(originals);
        Map<String, Map<String, Object>> indexMap = uniqueIndex(deserList(data), i -> i.keySet().iterator().next());
        for (String index : expected)
            assertThat(data, indexMap.containsKey(index), is(true));
    }

    @Test
    public void validateEmptyIndices() {
        String data = admin.getIndex("non-existent");
        assertThat(data, is("[]"));
    }

    @Test
    public void updateAlias() {
        String[] NEW_INDICES = new String[]{"new.test.live", "new.test.history"};
        admin.createIndex(IndexSetting.with(3, 1), singletonList(ADMIN_ALIAS), ADMIN_INDICES);
        admin.createIndex(IndexSetting.with(3, 1), emptyList(), NEW_INDICES);
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
        Map<String, String> originalMap = query.indexStatus(ADMIN_ALIAS);
        assertSettings(originalMap, "1", null);

        admin.updateSetting(settingBuilder().numberOfReplicas(2).refreshInterval(-1).build(), ADMIN_ALIAS);
        Map<String, String> updatedMap = query.indexStatus(ADMIN_ALIAS);
        assertSettings(updatedMap, "2", "-1");
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
        TypeMapping seqMapping = typeBuilder().index(ADMIN_INDICES[0]).type(ADMIN_TYPE).enabled(false).enableAll(false)
           .enableSource(false).enableType(false).build();
        admin.createType(seqMapping);
        for (long counter = 0; counter < 10; counter++) {
            long sequence = indexer.generateSequence(keyWith(ADMIN_INDICES[0], ADMIN_TYPE, "Sequence"));
            assertEquals(sequence, counter + 1);
        }
        admin.createType(seqMapping);
        assertEquals(indexer.generateSequence(keyWith(ADMIN_INDICES[0], ADMIN_TYPE, "Sequence")), 11);
    }

    @Test
    public void generateBlockSequences() {
        admin.createIndex(settingBuilder().numberOfShards(1).autoExpandReplicas().build(), ADMIN_INDICES[0]);
        admin.createType(typeBuilder().index(ADMIN_INDICES[0]).type(ADMIN_TYPE).enabled(false).enableAll(false)
           .enableSource(false).enableType(false).build());
        List<Long> sequences = indexer.generateSequences(keyWith(ADMIN_INDICES[0], ADMIN_TYPE, "Sequence"), 10);
        assertEquals(sequences, ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
    }
}
