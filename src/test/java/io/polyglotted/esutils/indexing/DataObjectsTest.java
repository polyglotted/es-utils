package io.polyglotted.esutils.indexing;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.esutils.query.request.QueryHints;
import io.polyglotted.esutils.query.request.SimpleSort;
import io.polyglotted.esutils.query.response.Flattened;
import io.polyglotted.esutils.query.response.SimpleDoc;
import org.testng.annotations.Test;

import static io.polyglotted.esutils.indexing.FieldMapping.notAnalyzedStringField;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class DataObjectsTest {

    @Test
    public void fieldTypeValues() {
        asList(FieldType.values()).contains(FieldType.valueOf("BINARY"));
    }

    @Test
    public void indexedValues() {
        asList(FieldMapping.Indexed.values()).contains(FieldMapping.Indexed.valueOf("NOT_ANALYZED"));
    }

    @Test
    public void hintOptionValues() {
        asList(QueryHints.Options.values()).contains(QueryHints.Options.valueOf("strictExpandOpen"));
    }

    @Test
    public void hintTypeValues() {
        asList(QueryHints.SearchType.values()).contains(QueryHints.SearchType.valueOf("query_and_fetch"));
    }

    @Test
    public void sortOrderValues() {
        asList(SimpleSort.Order.values()).contains(SimpleSort.Order.valueOf("ASC"));
        assertNotEquals(SimpleSort.sortAsc("a").order, SimpleSort.sortDesc("a").order);
    }

    @Test
    public void sortModeValues() {
        asList(SimpleSort.Mode.values()).contains(SimpleSort.Mode.valueOf("SUM"));
        assertEquals(SimpleSort.Mode.AVG.toMode(), "avg");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void aliasWithNoAliases() {
        Alias.aliasBuilder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void aliasWithNoIndices() {
        Alias.aliasBuilder().alias("hello").build();
    }

    @Test
    public void fieldMappingEqHash() {
        FieldMapping orig = notAnalyzedStringField("a").build();
        FieldMapping copy = notAnalyzedStringField("a").build();
        FieldMapping other = notAnalyzedStringField("c").build();
        verifyEqualsHashCode(orig, copy, other);
        verifyComparable(orig, other);
    }

    @Test
    public void indexKeyEqHash() {
        IndexKey orig = new IndexKey("a", "b", "c", 1);
        IndexKey copy = new IndexKey("a", "b", "c", 1);
        IndexKey other1 = new IndexKey("d", "b", "c", 1);
        IndexKey other2 = new IndexKey("a", "b", "d", 1);
        IndexKey other3 = new IndexKey("a", "b", "c", 2);
        verifyEqualsHashCode(orig, copy, other1, other2, other3);
        verifyComparable(orig, other1);
    }

    @Test
    public void simpleDocEqHash() {
        SimpleDoc orig = new SimpleDoc(new IndexKey("a", "b", "c", 1), ImmutableMap.of("a", "aa"));
        SimpleDoc copy = new SimpleDoc(new IndexKey("a", "b", "c", 1), ImmutableMap.of("a", "aa"));
        SimpleDoc other1 = new SimpleDoc(new IndexKey("a", "b", "d", 1), ImmutableMap.of("a", "aa"));
        SimpleDoc other2 = new SimpleDoc(new IndexKey("a", "b", "c", 1), ImmutableMap.of("a", "bb"));
        verifyEqualsHashCode(orig, copy, other1, other2);
    }

    @Test
    public void flattenedEqHash() {
        Flattened orig = new Flattened("a", "b", 1, 0L);
        Flattened copy = new Flattened("a", "b", 1, 0L);
        Flattened other1 = new Flattened("a", "b", 1, 1L);
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
        for(int i = 0; i < others.length; ++i) {
            assertNotEquals(obj, others[i]);
        }
    }

    @SafeVarargs
    public static <T extends Comparable<T>> void verifyComparable(T obj1, T... others) {
        assertEquals(obj1.compareTo(null), -1);
        assertEquals(obj1.compareTo(obj1), 0);
        for(T other : others) {
            assertTrue(obj1.compareTo(other) < 0);
        }
    }
}