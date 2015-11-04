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
import static io.polyglotted.eswrapper.indexing.FieldType.*;
import static io.polyglotted.eswrapper.indexing.TransformScript.scriptBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IndexSerializerTest extends IndexSerializer {
    public static java.util.Map<String, String> SERIALISED_DOCS = readAllDocs("files/SerialisedDocs.txt");

    public static TypeMapping completeTypeMapping(String index) {
        return typeBuilder().index(index).type("testType")
           .fieldMapping(notAnalyzedField("field01", BOOLEAN))
           .fieldMapping(notAnalyzedField("field02", STRING))
           .fieldMapping(notAnalyzedField("field03", FLOAT))
           .fieldMapping(notAnalyzedField("field04", DOUBLE))
           .fieldMapping(notAnalyzedField("field05", BYTE))
           .fieldMapping(notAnalyzedField("field06", SHORT))
           .fieldMapping(notAnalyzedField("field07", INTEGER))
           .fieldMapping(notAnalyzedField("field08", LONG))
           .fieldMapping(notAnalyzedField("field09", DATE))
           .fieldMapping(notAnalyzedField("field10", BINARY))
           .fieldMapping(notAnalyzedField("field11", IP))
           .fieldMapping(notAnalyzedField("field12", GEO_POINT))
           .fieldMapping(notAnalyzedField("field13", GEO_SHAPE))
           .fieldMapping(notAnalyzedField("field14", OBJECT))
           .build();
    }

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
    public void completeTypeMapping() {
        String actual = GSON.toJson(completeTypeMapping("testIndex"));
        //System.out.println("completeTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("completeTypeMapping")));
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
           .transform(scriptBuilder().script("ctx._source['field2'] = ctx._source['field1']")
              .lang("groovy").param("attr1", "attr2")).build());
        //System.out.println("transformTypeMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("transformTypeMapping")));
    }

    @Test
    public void multiTransformTypeMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("testType")
           .fieldMapping(notAnalyzedStringField("field1")).fieldMapping(notAnalyzedStringField("field2"))
           .transform(scriptBuilder().script("ctx._source['field2'] = ctx._source['field1']"))
           .transform(scriptBuilder().script("ctx._source['field3'] = ctx._source['field1']")).build());
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
    public void withAnalyzerMapping() {
        String actual = GSON.toJson(typeBuilder().index("testIndex").type("TestObj")
           .fieldMapping(notAnalyzedStringField("name"))
           .fieldMapping(simpleField("value", STRING).analyzer("whitespace")).build());
        //System.out.println("withAnalyzerMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("withAnalyzerMapping")));
    }

    @Test
    public void sequenceMapping() {
        String actual = TypeMapping.forcedMappingJson("Sequence");
        //System.out.println("sequenceMapping=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("sequenceMapping")));
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
    public void autoExpandSetting() {
        String actual = IndexSetting.settingBuilder().numberOfShards(1).autoExpandReplicas().build().createJson();
        //System.out.println("autoExpandSetting=" + actual);
        assertThat(actual, is(SERIALISED_DOCS.get("autoExpandSetting")));
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