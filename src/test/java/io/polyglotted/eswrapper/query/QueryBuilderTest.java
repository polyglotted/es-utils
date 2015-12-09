package io.polyglotted.eswrapper.query;

import io.polyglotted.pgmodel.search.query.SortMode;
import io.polyglotted.pgmodel.search.query.SortOrder;
import io.polyglotted.pgmodel.search.query.StandardQuery;
import org.testng.annotations.Test;

import static io.polyglotted.pgmodel.search.query.Expressions.equalsTo;
import static io.polyglotted.pgmodel.search.query.QueryHints.hintsBuilder;
import static io.polyglotted.pgmodel.search.query.SearchOptions.STRICT_EXPAND_OPEN;
import static io.polyglotted.pgmodel.search.query.SearchType.COUNT;
import static io.polyglotted.pgmodel.search.query.Sort.sortBuilder;
import static io.polyglotted.pgmodel.search.query.StandardQuery.queryBuilder;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToJson;
import static org.testng.Assert.assertEquals;

public class QueryBuilderTest extends QueryBuilder {

    @Test
    public void testHintsAndFrom() throws Exception {
        StandardQuery query = queryBuilder().field("b").offset(10).hints(hintsBuilder().indicesOptions(STRICT_EXPAND_OPEN)
           .fetchSource(false).preference("").routing("a").searchType(COUNT).timeoutInSeconds(10)).build();
        assertEquals(convertToJson(queryToRequest(query, null).source(), false, false), "{\"from\":10,\"size\":10,\"" +
           "timeout\":10000,\"query\":{\"match_all\":{}},\"version\":true,\"_source\":false,\"fields\":[\"b\",\"_parent\"]}");
    }

    @Test
    public void testSortAndScroll() throws Exception {
        StandardQuery query = queryBuilder().expression(equalsTo("a", "a"), equalsTo("b", "b"))
           .scrollTimeInMillis(3000L).sort(sortBuilder().field("a").missing("hi").order(SortOrder.DESC)
              .unmappedType("type").mode(SortMode.NONE)).build();
        assertEquals(convertToJson(queryToRequest(query, null).source(), false, false), "{\"size\":10,\"timeout\":10000," +
           "\"query\":{\"constant_score\":{\"filter\":{\"and\":{\"filters\":[{\"term\":{\"a\":\"a\"}},{\"term\":" +
           "{\"b\":\"b\"}}]}}}},\"version\":true,\"_source\":{\"includes\":[],\"excludes\":[]},\"fields\":" +
           "\"_parent\",\"sort\":[{\"a\":{\"order\":\"desc\",\"missing\":\"hi\",\"unmapped_type\":\"type\"}}]}");
    }
}