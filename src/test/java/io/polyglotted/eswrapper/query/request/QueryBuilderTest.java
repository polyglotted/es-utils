package io.polyglotted.eswrapper.query.request;

import io.polyglotted.eswrapper.query.StandardQuery;
import io.polyglotted.eswrapper.query.request.QueryHints.SearchOptions;
import io.polyglotted.eswrapper.query.request.Sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.Test;

import static io.polyglotted.eswrapper.query.StandardQuery.queryBuilder;
import static io.polyglotted.eswrapper.query.request.Expressions.equalsTo;
import static io.polyglotted.eswrapper.query.request.QueryHints.SearchOptions.STRICT_EXPAND_OPEN;
import static io.polyglotted.eswrapper.query.request.QueryHints.SearchType.COUNT;
import static io.polyglotted.eswrapper.query.request.QueryHints.hintsBuilder;
import static io.polyglotted.eswrapper.query.request.Sort.sortBuilder;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToJson;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class QueryBuilderTest extends QueryBuilder {

    @Test
    public void testOptionsFrom() throws Exception {
        for(SearchOptions options : SearchOptions.values()) {
            assertNotNull(options.toOptions());
        }
        assertEquals(SearchOptions.valueOf("LENIENT_EXPAND_OPEN").toOptions(), lenientExpandOpen());
    }

    @Test
    public void testHintsAndFrom() throws Exception {
        StandardQuery query = queryBuilder().field("b").offset(10).hints(hintsBuilder().indicesOptions(STRICT_EXPAND_OPEN)
           .fetchSource(false).preference("").routing("a").searchType(COUNT).timeoutInSeconds(10)).build();
        assertEquals(convertToJson(queryToRequest(query, null).source(), false, false), "{\"from\":10,\"size\":10," +
           "\"timeout\":10000,\"query\":{\"match_all\":{}},\"version\":true,\"_source\":false,\"fields\":\"b\"}");
    }

    @Test
    public void testSortAndScroll() throws Exception {
        StandardQuery query = queryBuilder().expression(equalsTo("a", "a"), equalsTo("b", "b"))
           .scrollTimeInMillis(3000L).sort(sortBuilder().field("a").missing("hi").order(SortOrder.DESC)
              .unmappedType("type").mode(SortMode.NONE)).build();
        assertEquals(convertToJson(queryToRequest(query, null).source(), false, false), "{\"size\":10," +
           "\"timeout\":10000,\"query\":{\"constant_score\":{\"filter\":{\"and\":{\"filters\":[{\"term\":" +
           "{\"a\":\"a\"}},{\"term\":{\"b\":\"b\"}}]}}}},\"version\":true,\"sort\":[{\"a\":{\"order\":\"desc\"," +
           "\"missing\":\"hi\",\"unmapped_type\":\"type\"}}]}");
    }
}