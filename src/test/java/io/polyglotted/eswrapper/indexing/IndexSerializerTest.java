package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.List;

import static io.polyglotted.eswrapper.indexing.FieldMapping.fieldBuilder;
import static io.polyglotted.eswrapper.indexing.FieldMapping.nestedField;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedField;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.eswrapper.indexing.FieldMapping.simpleField;
import static io.polyglotted.eswrapper.indexing.FieldType.BINARY;
import static io.polyglotted.eswrapper.indexing.FieldType.BOOLEAN;
import static io.polyglotted.eswrapper.indexing.FieldType.STRING;
import static io.polyglotted.eswrapper.indexing.TransformScript.transformBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IndexSerializerTest extends IndexSerializer {
    static java.util.Map<String, String> SERIALISED_DOCS = readAllDocs("files/SerialisedDocs.txt");

    @Test
    public void fieldMapping() {
        ImmutableMap<String, Object> mapping = notAnalyzedStringField("field1").build().mapping;
        assertThat(mapping.get("type"), is(equalTo("string")));
        assertThat(mapping.get("index"), is(equalTo("not_analyzed")));
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
        GSON.toJson(typeBuilder().index("testIndex").type("testType").build());
    }

    @Test
    public void simpleTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .fieldMapping(notAnalyzedStringField("field1").stored(true)).build());
        //System.out.println("simpleTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("simpleTypeMapping")));
    }

    @Test
    public void simpleFieldTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .fieldMapping(fieldBuilder().field("field1").type(BINARY)).build());
        //System.out.println("simpleFieldTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("simpleFieldTypeMapping")));
    }

    @Test
    public void strictNoSourceTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .strict(true).storeSource(false).fieldMapping(notAnalyzedStringField("field1"))
           .fieldMapping(notAnalyzedStringField("field2")).build());
        //System.out.println("strictNoSourceTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("strictNoSourceTypeMapping")));
    }

    @Test
    public void sourceIncludesTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .fieldMapping(notAnalyzedField("field1", STRING).includeInSource(true)).build());
        //System.out.println("sourceIncludesTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("sourceIncludesTypeMapping")));
    }

    @Test
    public void transformTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .fieldMapping(notAnalyzedStringField("field1")).fieldMapping(notAnalyzedStringField("field2"))
           .transform(transformBuilder().script("ctx._source['field2'] = ctx._source['field1']")
              .lang("groovy").param("attr1", "attr2")).build());
        //System.out.println("transformTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("transformTypeMapping")));
    }

    @Test
    public void multiTransformTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .fieldMapping(notAnalyzedStringField("field1")).fieldMapping(notAnalyzedStringField("field2"))
           .transform(transformBuilder().script("ctx._source['field2'] = ctx._source['field1']"))
           .transform(transformBuilder().script("ctx._source['field3'] = ctx._source['field1']")).build());
        //System.out.println("multiTransformTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("multiTransformTypeMapping")));
    }

    @Test
    public void setAsPathTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .fieldMapping(notAnalyzedStringField("name"))
           .fieldMapping(notAnalyzedStringField("path").isAPath(true)).build());
        //System.out.println("setAsPathTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("setAsPathTypeMapping")));
    }

    @Test
    public void metaTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .fieldMapping(notAnalyzedStringField("field1")).metaData("myName", "myVal").build());
        //System.out.println("metaTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("metaTypeMapping")));
    }

    @Test
    public void nestedMapping() {
        FieldMapping.Builder constraint = nestedField("constraint").property(asList(notAnalyzedField("attr", STRING),
           notAnalyzedField("func", STRING), notAnalyzedField("val", STRING), notAnalyzedField("neg", BOOLEAN)));
        FieldMapping.Builder axiom = nestedField("axiom").property(asList(notAnalyzedField("effect", STRING), constraint));

        String actual = GSON.toJson(typeBuilder().index("testIndex").type("NestedObj")
           .fieldMapping(notAnalyzedStringField("target").isAPath(true)).fieldMapping(axiom).build());
        //System.out.println("nestedMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("nestedMapping")));
    }

    @Test
    public void emptyNestedMapping() {
        FieldMapping.Builder axiom = nestedField("axiom").property(asList(notAnalyzedField("effect", STRING),
           nestedField("emptyField")));

        String actual = GSON.toJson(typeBuilder().index("testIndex").type("NestedObj")
           .fieldMapping(notAnalyzedStringField("target").isAPath(true)).fieldMapping(axiom).build());
        //System.out.println("emptyNestedMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("emptyNestedMapping")));
    }

    @Test
    public void withIndexerMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("TestObj")
           .fieldMapping(notAnalyzedStringField("name"))
           .fieldMapping(simpleField("value", STRING).indexer("whitespace")).build());
        //System.out.println("withIndexerMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("withIndexerMapping")));
    }

    @Test
    public void defaultIndexSetting() {
        String actual = IndexSetting.with(5, 1).createJson();
        //System.out.println("defaultIndexSetting=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("defaultIndexSetting")));
    }

    @Test
    public void forcedIndexSetting() {
        String actual = IndexSetting.settingBuilder().numberOfShards(3).numberOfReplicas(2).refreshInterval(-1L)
           .any("translog.disable_flush", true).disableDynamicMapping().ignoreMalformed().build().createJson();
        //System.out.println("forcedIndexSetting=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("forcedIndexSetting")));
    }

    @Test
    public void settingForUpdate() {
        String actual = IndexSetting.with(5, 1).updateJson();
        assertThat(actual, is(SERIALISED_DOCS.get("settingForUpdate")));
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