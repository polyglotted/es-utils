package io.polyglotted.eswrapper.indexing;

import org.testng.annotations.Test;

import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedStringField;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class DataObjectsTest {

    @Test
    public void indexActionValues() {
        asList(RecordAction.values()).contains(RecordAction.valueOf("CREATE"));
    }

    @Test
    public void indexSettingEqHash() {
        IndexSetting orig = IndexSetting.with(3, 0);
        IndexSetting copy = IndexSetting.with(3, 0);
        IndexSetting other = IndexSetting.with(1, 0);
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void typeMappingEqHash() throws Exception {
        TypeMapping orig = typeBuilder().index("a").type("a").fieldMapping(notAnalyzedStringField("a")).build();
        TypeMapping copy = typeBuilder().index("a").type("a").fieldMapping(notAnalyzedStringField("a")).build();
        TypeMapping other = typeBuilder().index("b").type("b").fieldMapping(notAnalyzedStringField("a")).build();
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void indexRecordEqHash() {
        IndexRecord orig = IndexRecord.createRecord("a", "b", "c").source("").build();
        IndexRecord copy = IndexRecord.createRecord("a", "b", "c").source("").build();
        IndexRecord other1 = IndexRecord.createRecord("a", "b", "d").source("").build();
        verifyEqualsHashCode(orig, copy, other1);
    }

    @SafeVarargs
    public static <T> void verifyEqualsHashCode(T obj, T copy, T... others) {
        assertNotNull(obj.toString());
        assertEquals(obj, obj);
        assertEquals(obj, copy);
        assertEquals(obj.hashCode(), copy.hashCode());
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(""));
        for (T other : others) {
            assertNotEquals(obj, other);
        }
    }
}