package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.List;

import static io.polyglotted.eswrapper.indexing.FieldMapping.fieldBuilder;
import static io.polyglotted.eswrapper.indexing.FieldMapping.nestedField;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedField;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.eswrapper.indexing.FieldMapping.objectField;
import static io.polyglotted.eswrapper.indexing.FieldMapping.simpleField;
import static io.polyglotted.eswrapper.indexing.FieldType.*;
import static io.polyglotted.eswrapper.indexing.TransformScript.scriptBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IndexSerializerTest extends IndexSerializer {
    public static java.util.Map<String, String> SERIALISED_DOCS = readAllDocs("files/SerialisedDocs.txt");

    public static TypeMapping.Builder completeTypeMapping(String index) {
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
           .fieldMapping(nestedField("field11").property(singleton(notAnalyzedStringField("inner1"))))
           .fieldMapping(notAnalyzedField("field12", IP))
           .fieldMapping(notAnalyzedField("field13", GEO_POINT))
           .fieldMapping(notAnalyzedField("field14", GEO_SHAPE))
           .fieldMapping(objectField("field15").property(singleton(notAnalyzedStringField("inner2"))));
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

    @DataProvider
    public static Object[][] typeMappingInputs() {
        return new Object[][]{
           {typeBuilder().index("testIndex").type("testType").allEnabled(null), "emptyTypeMapping"},

           {typeBuilder().index("testIndex").type("testType").fieldMapping(notAnalyzedStringField("field1").stored(true)), "simpleTypeMapping"},

           {typeBuilder().index("testIndex").type("testType").fieldMapping(fieldBuilder().field("field1").type(BINARY)), "simpleFieldTypeMapping"},

           {completeTypeMapping("testIndex"), "completeTypeMapping"},

           {typeBuilder().index("testIndex").type("testType").strict(true).storeSource(false).fieldMapping(notAnalyzedStringField("field1"))
              .fieldMapping(notAnalyzedStringField("field2")), "strictNoSourceTypeMapping"},

           {typeBuilder().index("testIndex").type("testType").fieldMapping(notAnalyzedField("field1", STRING).includeInSource(true)), "sourceIncludesTypeMapping"},

           {typeBuilder().index("testIndex").type("testType")
              .fieldMapping(notAnalyzedStringField("field1")).fieldMapping(notAnalyzedStringField("field2")).transform(scriptBuilder()
                 .script("ctx._source['field2'] = ctx._source['field1']").lang("groovy").param("attr1", "attr2")), "transformTypeMapping"},

           {typeBuilder().index("testIndex").type("testType").fieldMapping(notAnalyzedStringField("field1")).fieldMapping(notAnalyzedStringField("field2"))
              .transform(scriptBuilder().script("ctx._source['field2'] = ctx._source['field1']"))
              .transform(scriptBuilder().script("ctx._source['field3'] = ctx._source['field1']")), "multiTransformTypeMapping"},

           {typeBuilder().index("testIndex").type("testType").fieldMapping(notAnalyzedStringField("name"))
              .fieldMapping(notAnalyzedStringField("path").isAPath(true)), "setAsPathTypeMapping"},

           {typeBuilder().index("testIndex").type("testType").fieldMapping(notAnalyzedStringField("name"))
              .fieldMapping(simpleField("rawable", STRING).addRawFields()), "addRawFieldsTypeMapping"},

           {typeBuilder().index("testIndex").type("testType").fieldMapping(notAnalyzedStringField("field1")).metaData("myName", "myVal"), "metaTypeMapping"},

           {typeBuilder().index("testIndex").type("NestedObj").fieldMapping(notAnalyzedStringField("target").isAPath(true)).fieldMapping(nestedField("axiom")
              .property(asList(notAnalyzedField("effect", STRING), nestedField("constraint").property(asList(notAnalyzedField("attr", STRING),
                 notAnalyzedField("func", STRING), notAnalyzedField("val", STRING), notAnalyzedField("neg", BOOLEAN)))))), "nestedMapping"},

           {typeBuilder().index("testIndex").type("NestedObj").fieldMapping(notAnalyzedStringField("target").isAPath(true)).fieldMapping(
              nestedField("axiom").property(asList(notAnalyzedField("effect", STRING), nestedField("emptyField")))), "emptyNestedMapping"},

           {typeBuilder().index("testIndex").type("TestObj").fieldMapping(notAnalyzedStringField("name")).fieldMapping(
              simpleField("value", STRING).analyzer("whitespace")), "withAnalyzerMapping"},

           {typeBuilder().index("testIndex").type("testType").allEnabled(false).allAnalyzer(null), "disabledAllMapping"},

           {typeBuilder().index("testIndex").type("testType").allAnalyzer("my_analyzer"), "customAllMapping"},

           {typeBuilder().index("testIndex").type("Parent").allEnabled(false).allAnalyzer(null).fieldMapping(notAnalyzedStringField("target")
              .copyTo("freetext")).fieldMapping(simpleField("freetext", STRING).analyzer("all_analyzer"))
              .fieldMapping(nestedField("child").property(singletonList(notAnalyzedField("effect", STRING).copyTo("freetext")))), "copyToMapping"},
        };
    }

    @Test(dataProvider = "typeMappingInputs")
    public void validTypeMapping(TypeMapping.Builder type, String expectedKey) {
        String actual = GSON.toJson(type.build());
        assertThat(expectedKey + "=" + actual, actual, is(SERIALISED_DOCS.get(expectedKey)));
    }

    @Test(enabled = false)
    public void printAll() {
        Object[][] inputs = typeMappingInputs();
        for(Object[] items : inputs) {
            TypeMapping.Builder type = (TypeMapping.Builder) items[0];
            String expectedKey = (String) items[1];
            String actual = GSON.toJson(type.build());
            System.out.println(expectedKey + "=" + actual);
        }
    }

    @Test
    public void sequenceMapping() {
        String actual = TypeMapping.forcedMappingJson("Sequence");
        assertThat("sequenceMapping=" + actual, actual, is(SERIALISED_DOCS.get("sequenceMapping")));
    }

    @Test
    public void defaultIndexSetting() {
        String actual = IndexSetting.with(5, 1).createJson();
        assertThat("defaultIndexSetting=" + actual, actual, is(SERIALISED_DOCS.get("defaultIndexSetting")));
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