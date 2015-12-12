package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.polyglotted.eswrapper.AbstractElasticTest;
import org.testng.annotations.Test;

import java.util.List;

import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.fieldBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.nestedField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.objectField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.simpleField;
import static io.polyglotted.pgmodel.search.index.FieldType.*;
import static io.polyglotted.pgmodel.search.index.Script.scriptBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TypeMappingElasticTest extends AbstractElasticTest {
    private static final String TEST_INDEX = "test_types_index";

    public static TypeMapping.Builder completeTypeMapping(String index) {
        return typeBuilder().index(index).type("CompleteTypeMapping")
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

    public static List<TypeMapping.Builder> typeMappingInputs() {
        return ImmutableList.of(
           typeBuilder().index(TEST_INDEX).type("EmptyTypeMapping").enableAll(false).enableType(false),

           typeBuilder().index(TEST_INDEX).type("DynamicDisabledMapping").strict(true).enabled(false).enableType(false),

           typeBuilder().index(TEST_INDEX).type("ParentEnabled").enableSource(false)
              .parent("EmptyTypeMapping").enableType(false),

           typeBuilder().index(TEST_INDEX).type("SequenceMapping").enabled(false).enableAll(false)
              .enableSource(false).enableType(false),

           typeBuilder().index(TEST_INDEX).type("MetaTypeMapping").fieldMapping(notAnalyzedStringField("field1"))
              .metaData("myName", "myVal").parent("EmptyTypeMapping").strict(),

           typeBuilder().index(TEST_INDEX).type("StoredTypeMapping").fieldMapping(notAnalyzedStringField("field1")
              .stored(true)),

           typeBuilder().index(TEST_INDEX).type("SimpleFieldTypeMapping").fieldMapping(fieldBuilder().field("field1")
              .type(BINARY)),

           completeTypeMapping(TEST_INDEX),

           typeBuilder().index(TEST_INDEX).type("StrictNoSourceTypeMapping").strict(true).enableSource(false)
              .fieldMapping(notAnalyzedStringField("field1")).fieldMapping(notAnalyzedStringField("field2")),

           typeBuilder().index(TEST_INDEX).type("SourceIncludesTypeMapping")
              .fieldMapping(notAnalyzedField("field1", STRING)).include("field1"),

           typeBuilder().index(TEST_INDEX).type("TransformTypeMapping").fieldMapping(notAnalyzedStringField("field1"))
              .fieldMapping(notAnalyzedStringField("field2")).transform(scriptBuilder()
              .script("ctx._source['field2'] = ctx._source['field1']").lang("groovy").param("attr1", "attr2")),

           typeBuilder().index(TEST_INDEX).type("MultiTransformMapping").fieldMapping(notAnalyzedStringField("field1"))
              .fieldMapping(notAnalyzedStringField("field2"))
              .transform(scriptBuilder().script("ctx._source['field2'] = ctx._source['field1']"))
              .transform(scriptBuilder().script("ctx._source['field3'] = ctx._source['field1']")),

           typeBuilder().index(TEST_INDEX).type("PathTypeMapping").fieldMapping(notAnalyzedStringField("name"))
              .fieldMapping(notAnalyzedStringField("path").isAPath()),

           typeBuilder().index(TEST_INDEX).type("RawFieldsTypeMapping").fieldMapping(notAnalyzedStringField("name"))
              .fieldMapping(simpleField("rawable", STRING).addRawFields()),

           typeBuilder().index(TEST_INDEX).type("NestedMapping").fieldMapping(notAnalyzedStringField("target")
              .isAPath()).fieldMapping(nestedField("axiom").property(asList(notAnalyzedField("effect", STRING),
              nestedField("constraint").property(asList(notAnalyzedField("attr", STRING),
                 notAnalyzedField("func", STRING), notAnalyzedField("val", STRING),
                 notAnalyzedField("neg", BOOLEAN)))))),

           typeBuilder().index(TEST_INDEX).type("EmptyNestedMapping").fieldMapping(notAnalyzedStringField("target")
              .isAPath()).fieldMapping(nestedField("axiom").property(asList(notAnalyzedField("effect", STRING),
              nestedField("emptyField")))),

           typeBuilder().index(TEST_INDEX).type("WithAnalyzerMapping").fieldMapping(notAnalyzedStringField("name"))
              .fieldMapping(simpleField("value", STRING).analyzer("whitespace")),

           typeBuilder().index(TEST_INDEX).type("DisableAllMapping").enableAll(false),

           typeBuilder().index(TEST_INDEX).type("CustomAnalyzerMapping").allAnalyzer("keyword"),

           typeBuilder().index(TEST_INDEX).type("CopyToMapping").allAnalyzer(null)
              .fieldMapping(notAnalyzedStringField("target").copyTo("freetext")).fieldMapping(
              simpleField("freetext", STRING).analyzer("all_analyzer")).fieldMapping(nestedField("child")
              .property(singletonList(notAnalyzedField("effect", STRING).copyTo("freetext"))))
        );
    }

    @Test
    public void testTypeMapping() {
        admin.dropIndex(TEST_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), TEST_INDEX);
        try {
            List<TypeMapping> typeMappings = Lists.transform(typeMappingInputs(), TypeMapping.Builder::build);
            typeMappings.forEach(admin::createType);
            for (TypeMapping mapping : typeMappings) {
                String type = mapping.type;
                String actual = admin.getMapping(TEST_INDEX, type);
                assertThat(actual, actual, is(mapping.mappingJson()));
            }
        } finally {
            admin.dropIndex(TEST_INDEX);
        }
    }
}
