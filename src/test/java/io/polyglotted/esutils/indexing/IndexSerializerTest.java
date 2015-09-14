package io.polyglotted.esutils.indexing;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.List;

import static io.polyglotted.esutils.indexing.FieldType.BINARY;
import static io.polyglotted.esutils.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.esutils.indexing.FieldMapping.privateField;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IndexSerializerTest extends IndexSerializer {
    static java.util.Map<String, String> SERIALISED_DOCS = readAllDocs("files/SerialisedDocs.txt");

    @Test
    public void fieldMapping() {
        ImmutableMap<String, Object> mapping = privateField("field1").mapping;
        assertThat(mapping.get("type"), is(equalTo("string")));
        assertThat(mapping.get("index"), is(equalTo("not_analyzed")));
        assertThat(mapping.get("include_in_all"), is(equalTo(false)));
        assertThat(mapping.get("doc_values"), is(equalTo(true)));
    }

    @Test
    public void fieldMappingCustom() {
        ImmutableMap<String, Object> mapping = new FieldMapping("location", false,
                "{\"type\": \"geo_shape\",\"tree\": \"quadtree\",\"precision\": \"1m\"}").mapping;
        assertThat(mapping.get("type"), is(equalTo("geo_shape")));
        assertThat(mapping.get("tree"), is(equalTo("quadtree")));
        assertThat(mapping.get("precision"), is(equalTo("1m")));
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "atleast one field must be indexed")
    public void noFieldTypeMapping() {
        GSON.toJson(TypeMapping.builder().index("testIndex").type("testType").build());
    }

    @Test
    public void simpleTypeMapping() {
        String actual = GSON.toJson(TypeMapping.builder().index("testIndex").type("testType")
                .fieldMapping(notAnalyzedStringField("field1").stored(true)).build());
        assertThat(actual, is(SERIALISED_DOCS.get("simpleTypeMapping")));
    }

    @Test
    public void simpleFieldTypeMapping() {
        String actual = GSON.toJson(TypeMapping.builder().index("testIndex").type("testType")
                .fieldMapping(FieldMapping.builder().field("field1").type(BINARY)).build());
        assertThat(actual, is(SERIALISED_DOCS.get("simpleFieldTypeMapping")));
    }

    @Test
    public void strictNoSourceTypeMapping() {
        String actual = GSON.toJson(TypeMapping.builder().index("testIndex").type("testType")
                .strict(true).storeSource(false).fieldMapping(notAnalyzedStringField("field1"))
                .fieldMapping(notAnalyzedStringField("field2")).build());
        assertThat(actual, is(SERIALISED_DOCS.get("strictNoSourceTypeMapping")));
    }

    @Test
    public void sourceIncludesTypeMapping() {
        String actual = GSON.toJson(TypeMapping.builder().index("testIndex").type("testType")
                .fieldMapping(privateField("field1")).build());
        assertThat(actual, is(SERIALISED_DOCS.get("sourceIncludesTypeMapping")));
    }

    @Test
    public void transformTypeMapping() {
        String actual = GSON.toJson(TypeMapping.builder().index("testIndex").type("testType")
                .fieldMapping(notAnalyzedStringField("field1")).fieldMapping(notAnalyzedStringField("field2"))
                .transform(TransformScript.builder().script("ctx._source['field2'] = ctx._source['field1']")
                        .lang("groovy").param("attr1", "attr2")).build());
        assertThat(actual, is(SERIALISED_DOCS.get("transformTypeMapping")));
    }

    @Test
    public void multiTransformTypeMapping() {
        String actual = GSON.toJson(TypeMapping.builder().index("testIndex").type("testType")
                .fieldMapping(notAnalyzedStringField("field1")).fieldMapping(notAnalyzedStringField("field2"))
                .transform(TransformScript.builder().script("ctx._source['field2'] = ctx._source['field1']"))
                .transform(TransformScript.builder().script("ctx._source['field3'] = ctx._source['field1']")).build());
        assertThat(actual, is(SERIALISED_DOCS.get("multiTransformTypeMapping")));
    }

    @Test
    public void metaTypeMapping() {
        String actual = GSON.toJson(TypeMapping.builder().index("testIndex").type("testType")
                .fieldMapping(notAnalyzedStringField("field1")).metaData("myName", "myVal").build());
        assertThat(actual, is(SERIALISED_DOCS.get("metaTypeMapping")));
    }

    @Test
    public void defaultIndexSetting() {
        String actual = GSON.toJson(IndexSetting.defaultSetting());
        assertThat(actual, is(SERIALISED_DOCS.get("defaultIndexSetting")));
    }

    @Test
    public void forcedIndexSetting() {
        String actual = GSON.toJson(new IndexSetting(3, 2, -1L, true));
        assertThat(actual, is(SERIALISED_DOCS.get("forcedIndexSetting")));
    }

    private static java.util.Map<String, String> readAllDocs(String file) {
        java.util.Map<String, String> result = new HashMap<>();
        try {
            URL resource = IndexSerializerTest.class.getClassLoader().getResource(file);
            List<String> lines = Resources.readLines(resource, Charsets.UTF_8);
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