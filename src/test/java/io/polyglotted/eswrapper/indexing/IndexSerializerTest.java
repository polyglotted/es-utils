package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.eswrapper.indexing.IndexSetting.settingBuilder;
import static io.polyglotted.eswrapper.indexing.IndexSetting.with;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMappingElasticTest.completeTypeMapping;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IndexSerializerTest extends IndexSerializer {
    public static java.util.Map<String, String> SERIALISED_DOCS = readAllDocs("files/SerialisedDocs.txt");

    @DataProvider
    public static Object[][] indexSettingInputs() {
        return new Object[][]{
           {with(5, 1), "defaultIndexSetting"},
           {settingBuilder().numberOfShards(3).numberOfReplicas(2).refreshInterval(-1L).ignoreMalformed()
              .any("translog.disable_flush", true).disableDynamicMapping().any("foo", null).build(), "forcedIndexSetting"},
           {settingBuilder().numberOfShards(1).autoExpandReplicas().build(), "autoExpandSetting"},
        };
    }

    @Test(dataProvider = "indexSettingInputs")
    public void validIndexSetting(IndexSetting setting, String expectedKey) {
        String actual = setting.createJson();
        assertThat(expectedKey + "=" + actual, actual, is(SERIALISED_DOCS.get(expectedKey)));
    }

    @Test
    public void settingForUpdate() {
        String actual = with(5, 1).updateJson();
        assertThat(actual, is(SERIALISED_DOCS.get("settingForUpdate")));
    }

    @DataProvider
    public static Object[][] typeMappingInputs() {
        return new Object[][]{
           {typeBuilder().index("test_index").type("$lock").enableAll(false).enableSource(false)},

           {typeBuilder().index("test_index").type("SequenceMapping").enabled(false).enableAll(false)
              .enableSource(false).enableType(false)},

           {completeTypeMapping("test_index")},
        };
    }

    @Test(dataProvider = "typeMappingInputs")
    public void validTypeMapping(TypeMapping.Builder type) {
        TypeMapping mapping = type.build();
        String actual = GSON.toJson(mapping);
        assertThat(mapping.type + "=" + actual, actual, is(SERIALISED_DOCS.get(mapping.type)));
    }

    private static java.util.Map<String, String> readAllDocs(String file) {
        java.util.Map<String, String> result = new HashMap<>();
        try {
            URL resource = IndexSerializerTest.class.getClassLoader().getResource(file);
            List<String> lines = Resources.readLines(checkNotNull(resource), Charsets.UTF_8);
            for (String line : lines) {
                int eqIndex = line.indexOf("=");
                result.put(line.substring(0, eqIndex), line.substring(eqIndex + 1));
            }
        } catch (Exception ioe) {
            ioe.printStackTrace();
        }
        return result;
    }
}