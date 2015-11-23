package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.query.AggregationType;
import io.polyglotted.eswrapper.query.QueryResponse;
import io.polyglotted.eswrapper.query.StandardQuery;
import io.polyglotted.eswrapper.query.StandardScroll;
import io.polyglotted.eswrapper.query.request.Expression;
import io.polyglotted.eswrapper.query.request.Expressions;
import io.polyglotted.eswrapper.query.request.QueryHints;
import io.polyglotted.eswrapper.query.request.Sort;
import io.polyglotted.eswrapper.query.response.*;
import io.polyglotted.eswrapper.validation.Validity;
import org.testng.annotations.Test;

import static io.polyglotted.eswrapper.indexing.Alias.aliasBuilder;
import static io.polyglotted.eswrapper.indexing.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.eswrapper.indexing.IndexKey.keyWith;
import static io.polyglotted.eswrapper.indexing.Script.scriptBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.QueryResponse.responseBuilder;
import static io.polyglotted.eswrapper.query.StandardQuery.queryBuilder;
import static io.polyglotted.eswrapper.query.request.QueryHints.SearchType.SCAN;
import static io.polyglotted.eswrapper.query.request.QueryHints.hintsBuilder;
import static io.polyglotted.eswrapper.query.response.Aggregation.aggregationBuilder;
import static io.polyglotted.eswrapper.query.response.Bucket.bucketBuilder;
import static io.polyglotted.eswrapper.query.response.Flattened.flattened;
import static io.polyglotted.eswrapper.validation.Validity.valid;
import static io.polyglotted.eswrapper.validation.Validity.validityBuilder;
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
    public void indexActionValues() {
        asList(IndexRecord.Action.values()).contains(IndexRecord.Action.valueOf("CREATE"));
    }

    @Test
    public void indexedValues() {
        asList(FieldMapping.Indexed.values()).contains(FieldMapping.Indexed.valueOf("NOT_ANALYZED"));
    }

    @Test
    public void hintOptionValues() {
        asList(QueryHints.SearchOptions.values()).contains(QueryHints.SearchOptions.valueOf("STRICT_EXPAND_OPEN"));
    }

    @Test
    public void hintTypeValues() {
        asList(QueryHints.SearchType.values()).contains(QueryHints.SearchType.valueOf("QUERY_AND_FETCH"));
    }

    @Test
    public void sortModeValues() {
        asList(Sort.SortMode.values()).contains(Sort.SortMode.valueOf("SUM"));
        assertEquals(Sort.SortMode.AVG.toMode(), "avg");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void aliasWithNoAliases() {
        aliasBuilder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void aliasWithNoIndices() {
        aliasBuilder().alias("hello").build();
    }

    @Test
    public void indexSettingEqHash() {
        IndexSetting orig = IndexSetting.with(3, 0);
        IndexSetting copy = IndexSetting.with(3, 0);
        IndexSetting other = IndexSetting.with(1, 0);
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void aliasEqHash() {
        Alias orig = aliasBuilder().alias("a").index("a").build();
        Alias copy = aliasBuilder().alias("a").index("a").build();
        Alias other = aliasBuilder().alias("b").index("a").build();
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
    public void scriptEqHash() {
        Script orig = scriptBuilder().script("a=b").build();
        Script copy = scriptBuilder().script("a=b").build();
        Script other = scriptBuilder().script("c=d").build();
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void sleeveEqHash() {
        Sleeve<String> orig = new Sleeve<>(keyWith("a", "a"), "a", null, true);
        Sleeve<String> copy = new Sleeve<>(keyWith("a", "a"), "a", null, true);
        Sleeve<String> other = new Sleeve<>(keyWith("b", "b"), "a", null, true);
        Sleeve<String> other2 = new Sleeve<>(keyWith("a", "a"), "b", null, true);
        verifyEqualsHashCode(orig, copy, other, other2);
    }

    @Test
    public void fieldMappingEqHash() {
        FieldMapping orig = notAnalyzedStringField("a").build();
        FieldMapping copy = notAnalyzedStringField("a").build();
        FieldMapping other = new FieldMapping("b", false, (String) null);
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
    public void indexRecordEqHash() {
        IndexRecord orig = IndexRecord.createRecord("abc", "def").source("").build();
        IndexRecord copy = IndexRecord.createRecord("abc", "def").source("").build();
        IndexRecord other1 = IndexRecord.createRecord("abc", "ghi").source("").build();
        verifyEqualsHashCode(orig, copy, other1);
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
        Flattened orig = flattened("a", "b", 1, 0L);
        Flattened copy = flattened("a", "b", 1, 0L);
        Flattened other1 = flattened("a", "b", 1, 1L);
        verifyEqualsHashCode(orig, copy, other1);
    }

    @Test
    public void expressionSimpleEqHash() {
        Expression orig = Expressions.equalsTo("product", "Coffee");
        Expression copy = Expressions.equalsTo("product", "Coffee");
        Expression other1 = Expressions.greaterThan("product", "Coffee");
        Expression other2 = Expressions.equalsTo("beverage", "Coffee");
        Expression other3 = Expressions.equalsTo("product", "Tea");
        verifyEqualsHashCode(orig, copy, other1, other2, other3);
    }

    @Test
    public void expressionCompoundEqHash() {
        Expression orig = Expressions.not(Expressions.in("bets", "a", "b"));
        Expression copy = Expressions.not(Expressions.in("bets", "a", "b"));
        Expression other1 = Expressions.not(Expressions.in("cups", "a", "b"));
        Expression other2 = Expressions.or(Expressions.in("bets", "a", "b"));
        Expression other3 = Expressions.in("product", "Coffee", "Tea");
        verifyEqualsHashCode(orig, copy, other1, other2, other3);
    }

    @Test
    public void responseHeaderEqHash() {
        ResponseHeader orig = new ResponseHeader(0, 20, 10, "hello");
        ResponseHeader copy = new ResponseHeader(0, 20, 10, "hello");
        ResponseHeader nil = new ResponseHeader(0, 20, 10, null);
        ResponseHeader nilCopy = new ResponseHeader(0, 20, 10, null);
        ResponseHeader other1 = new ResponseHeader(0, 10, 10, "hello");
        ResponseHeader other2 = new ResponseHeader(0, 20, 20, "hello");
        ResponseHeader other3 = new ResponseHeader(0, 20, 10, "notsame");
        verifyEqualsHashCode(orig, copy, nil, other1, other2, other3);
        verifyEqualsHashCode(nil, nilCopy, other3);
    }

    @Test
    public void standardQueryEqHash() {
        StandardQuery orig = queryBuilder().build();
        StandardQuery copy = queryBuilder().build();
        StandardQuery other = queryBuilder().index("hello").build();
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void standardScrollEqHash() {
        StandardScroll orig = new StandardScroll("a", 1L);
        StandardScroll copy = new StandardScroll("a", 1L);
        StandardScroll other1 = new StandardScroll("b", 1L);
        StandardScroll other2 = new StandardScroll("a", 2L);
        verifyEqualsHashCode(orig, copy, other1, other2);
    }

    @Test
    public void queryResponseEqHash() {
        QueryResponse orig = responseBuilder().header(new ResponseHeader(0, 20, 10, "orig")).build();
        QueryResponse copy = responseBuilder().header(new ResponseHeader(0, 20, 10, "orig")).build();
        QueryResponse other = responseBuilder().header(new ResponseHeader(0, 20, 10, "other")).build();
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void aggregationEqHash() {
        Aggregation orig = aggregationBuilder().label("a").type(AggregationType.Avg).value("Avg", 25).build();
        Aggregation copy = aggregationBuilder().label("a").type(AggregationType.Avg).value("Avg", 25).build();
        Aggregation other = aggregationBuilder().label("b").type(AggregationType.Avg).value("Avg", 25).build();
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void bucketEqHash() {
        Bucket orig = bucketBuilder().key("a").keyValue(1).docCount(1L).build();
        Bucket copy = bucketBuilder().key("a").keyValue(1).docCount(1L).build();
        Bucket other = bucketBuilder().key("b").keyValue(1).docCount(1L).build();
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void hintsEqHash() {
        QueryHints orig = hintsBuilder().build();
        QueryHints copy = hintsBuilder().build();
        QueryHints other = hintsBuilder().searchType(SCAN).build();
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void sortEqHash() {
        Sort orig = Sort.sortAsc("a");
        Sort copy = Sort.sortAsc("a");
        Sort other = Sort.sortDesc("a");
        verifyEqualsHashCode(orig, copy, other);
    }

    @Test
    public void memoEqHash() {
        Validity.Memo orig  = new Validity.Memo("a", "b");
        Validity.Memo copy  = new Validity.Memo("a", "b");
        Validity.Memo other1  = new Validity.Memo("a", "c");
        Validity.Memo other2  = new Validity.Memo("c", "b");
        Validity.Memo other3  = new Validity.Memo("c", "d");
        verifyEqualsHashCode(orig, copy, other1, other2, other3);
    }

    @Test
    public void validityEqHash() {
        Validity orig = validityBuilder().memo("a", "b").build();
        Validity copy = validityBuilder().memo("a", "b").build();
        Validity other = valid();
        verifyEqualsHashCode(orig, copy, other);
    }

    @SafeVarargs
    public static <T> void verifyEqualsHashCode(T obj, T copy, T... others) {
        assertNotNull(obj.toString());
        assertEquals(obj, obj);
        assertEquals(obj, copy);
        assertEquals(obj.hashCode(), copy.hashCode());
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(""));
        for (int i = 0; i < others.length; ++i) {
            assertNotEquals(obj, others[i]);
        }
    }

    @SafeVarargs
    public static <T extends Comparable<T>> void verifyComparable(T obj1, T... others) {
        assertEquals(obj1.compareTo(null), -1);
        assertEquals(obj1.compareTo(obj1), 0);
        for (T other : others) {
            assertTrue(obj1.compareTo(other) < 0);
        }
    }
}