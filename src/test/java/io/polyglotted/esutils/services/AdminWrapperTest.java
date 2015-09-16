package io.polyglotted.esutils.services;

import io.polyglotted.esutils.AbstractElasticTest;
import io.polyglotted.esutils.indexing.IndexSetting;
import org.testng.annotations.Test;

import java.util.Map;

import static io.polyglotted.esutils.indexing.Alias.aliasBuilder;
import static io.polyglotted.esutils.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.esutils.indexing.TypeMapping.typeBuilder;
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

        String MAPPING = "{\"AdminType\":{\"_all\":{\"enabled\":true,\"analyzer\":\"all_analyzer" +
           "\"},\"properties\":{\"a\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":true}}}}";
        assertThat(admin.getMapping(ADMIN_INDICES[0], ADMIN_TYPE), is(MAPPING));
        assertThat(admin.getMapping(ADMIN_INDICES[1], ADMIN_TYPE), is(MAPPING));

        admin.dropIndex(ADMIN_ALIAS);

        assertThat(admin.indexExists(ADMIN_INDICES), is(false));
    }

    @Test
    public void updateAlias() {
        String[] NEW_INDICES = new String[]{"new.test.live", "new.test.history"};
        admin.createIndex(IndexSetting.with(3, 1), singletonList(ADMIN_ALIAS), ADMIN_INDICES);
        admin.createIndex(IndexSetting.with(3, 1), NEW_INDICES);

        admin.indexAliases(aliasBuilder().alias(ADMIN_ALIAS).index(ADMIN_INDICES).remove().build(),
           aliasBuilder().alias(ADMIN_ALIAS).index(NEW_INDICES).build());

        admin.dropIndex(ADMIN_ALIAS);

        assertThat(admin.indexExists(NEW_INDICES), is(false));
        assertThat(admin.indexExists(ADMIN_INDICES), is(true));
    }

    @Test(enabled = false)
    public void updateSetting() {
        //TODO FAILED
        admin.createIndex(IndexSetting.with(3, 1), singletonList(ADMIN_ALIAS), ADMIN_INDICES);
        Map<String, Map<String, String>> originalMap = query.indexStatus(ADMIN_INDICES);
        assertSettings(originalMap.get(ADMIN_INDICES[0]), "1", null, null);
        assertSettings(originalMap.get(ADMIN_INDICES[0]), "1", null, null);

        admin.updateIndex(IndexSetting.with(2, -1L, true), ADMIN_ALIAS);

        Map<String, Map<String, String>> updatedMap = query.indexStatus(ADMIN_INDICES);
        assertSettings(updatedMap.get(ADMIN_INDICES[0]), "2", "-1", "true");
        assertSettings(updatedMap.get(ADMIN_INDICES[0]), "2", "-1", "true");
    }

    private static void assertSettings(Map<String, String> settings, String replicas, String interval, String disable) {
        assertEquals(settings.get("index.number_of_shards"), "3");
        assertEquals(settings.get("index.number_of_replicas"), replicas);
        assertEquals(settings.get("index.disable_flush"), interval);
        assertEquals(settings.get("index.disable_flush"), disable);
    }
}
