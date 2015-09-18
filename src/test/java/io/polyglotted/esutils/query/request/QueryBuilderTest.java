package io.polyglotted.esutils.query.request;

import io.polyglotted.esutils.query.StandardQuery;
import org.testng.annotations.Test;

import static io.polyglotted.esutils.query.StandardQuery.queryBuilder;
import static io.polyglotted.esutils.query.request.Expressions.equalsTo;
import static io.polyglotted.esutils.query.request.QueryHints.Options.strictExpand;
import static io.polyglotted.esutils.query.request.QueryHints.SearchType.count;
import static io.polyglotted.esutils.query.request.QueryHints.hintsBuilder;
import static io.polyglotted.esutils.query.request.SimpleSort.sortBuilder;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.action.support.IndicesOptions.strictExpandOpen;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToJson;
import static org.testng.Assert.assertEquals;

public class QueryBuilderTest extends QueryBuilder {

    @Test
    public void testOptionsFrom() throws Exception {
        assertEquals(optionsFrom("strictExpandOpen"), strictExpandOpen());
        assertEquals(optionsFrom("unknownOption"), lenientExpandOpen());
    }

    @Test
    public void testHintsAndFrom() throws Exception {
        StandardQuery query = queryBuilder().field("b").offset(10).hints(hintsBuilder().indicesOptions(strictExpand)
           .fetchSource(false).preference("").routing("a").searchType(count).timeoutInSeconds(10)).build();
        assertEquals(convertToJson(queryToRequest(query, null).source(), false, false), "{\"from\":10,\"size\":10," +
           "\"timeout\":10000,\"query\":{\"match_all\":{}},\"_source\":false,\"fields\":\"b\"}");
    }

    @Test
    public void testSortAndScroll() throws Exception {
        StandardQuery query = queryBuilder().expression(equalsTo("a", "a"), equalsTo("b", "b"))
           .scrollTimeInMillis(3000L).sort(sortBuilder().field("a").missing("hi").order(SimpleSort.Order.DESC)
              .unmappedType("type").mode(SimpleSort.Mode.NONE)).build();
        assertEquals(convertToJson(queryToRequest(query, null).source(), false, false), "{\"size\":10,\"timeout\":10000," +
           "\"query\":{\"constant_score\":{\"filter\":{\"and\":{\"filters\":[{\"term\":{\"a\":\"a\"}},{\"term\":{\"b\"" +
           ":\"b\"}}]}}}},\"sort\":[{\"a\":{\"order\":\"desc\",\"missing\":\"hi\",\"unmapped_type\":\"type\"}}]}");
    }
}