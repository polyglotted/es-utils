package io.polyglotted.eswrapper.query;

import io.polyglotted.esmodel.api.Expression;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.google.common.collect.ImmutableMap.of;
import static io.polyglotted.esmodel.api.Expressions.equalsTo;
import static io.polyglotted.esmodel.api.query.Aggregates.*;
import static io.polyglotted.eswrapper.query.AggsConverter.*;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToJson;
import static org.testng.Assert.assertEquals;

public class AggregatesTest {

    @DataProvider
    public static Object[][] aggregationInputs() {
        return new Object[][]{
           {Max.buildFrom(max("hello", "world")), "{\"aggregations\":{\"hello\":{\"max\":{\"field\":\"world\"}}}}"},
           {Min.buildFrom(min("hello", "world")), "{\"aggregations\":{\"hello\":{\"min\":{\"field\":\"world\"}}}}"},
           {Sum.buildFrom(sum("hello", "world")), "{\"aggregations\":{\"hello\":{\"sum\":{\"field\":\"world\"}}}}"},
           {Avg.buildFrom(avg("hello", "world")), "{\"aggregations\":{\"hello\":{\"avg\":{\"field\":\"world\"}}}}"},
           {Count.buildFrom(count("hello", "world")), "{\"aggregations\":{\"hello\":{\"value_count\":{\"field\":\"world\"}}}}"},
           {Term.buildFrom(term("hello", "world")), "{\"aggregations\":{\"hello\":" +
              "{\"terms\":{\"field\":\"world\",\"size\":20,\"show_term_doc_count_error\":true}}}}"},
           {Term.buildFrom(term("hello", "world", 5)), "{\"aggregations\":{\"hello\":" +
              "{\"terms\":{\"field\":\"world\",\"size\":5,\"show_term_doc_count_error\":true}}}}"},
           {Term.buildFrom(term("hello", "world", 5, "count", true)), "{\"aggregations\":{\"hello\":" +
              "{\"terms\":{\"field\":\"world\",\"size\":5,\"show_term_doc_count_error\":true,\"order\":" +
              "{\"_count\":\"asc\"}}}}}"},
           {Term.buildFrom(term("hello", "world", 5, "TERM", false)), "{\"aggregations\":{\"hello\":" +
              "{\"terms\":{\"field\":\"world\",\"size\":5,\"show_term_doc_count_error\":true,\"order\":" +
              "{\"_term\":\"desc\"}}}}}"},
           {Term.buildFrom(Expression.withMap(AggsConverter.Term.name(), "hello",
              of("field", "world", "size", 5, "order", "count"))), "{\"aggregations\":{\"hello\":{\"terms\":{\"field" +
              "\":\"world\",\"size\":5,\"show_term_doc_count_error\":true,\"order\":{\"_count\":\"desc\"}}}}}"},
           {Term.buildFrom(term("hello", "world", 5, "foo.bar", true)), "{\"aggregations\":{\"hello\":{\"terms\":" +
              "{\"field\":\"world\",\"size\":5,\"show_term_doc_count_error\":true,\"order\":{\"foo.bar\":\"asc\"}}}}}"},
           {Statistics.buildFrom(stats("hello", "world")), "{\"aggregations\":{\"hello\":{\"stats\":{\"field\":\"world\"}}}}"},
           {DateHistogram.buildFrom(dateHistogram("hello", "world", "year")), "{\"aggregations\":{\"hello\"" +
              ":{\"date_histogram\":{\"field\":\"world\",\"interval\":\"year\",\"format\":\"yyyy-MM-dd\"}}}}"},
           {Filter.buildFrom(filterAggBuilder("hello", equalsTo("a", "b")).add(sumBuilder("x", "y")).build()),
              "{\"aggregations\":{\"hello\":{\"filter\":{\"term\":{\"a\":\"b\"}}," +
                 "\"aggregations\":{\"x\":{\"sum\":{\"field\":\"y\"}}}}}}"},
           {Children.buildFrom(childrenAggBuilder("hello", "world").add(sumBuilder("x", "y")).build()),
              "{\"aggregations\":{\"hello\":{\"children\":{\"type\":\"world\"}," +
                 "\"aggregations\":{\"x\":{\"sum\":{\"field\":\"y\"}}}}}}"},
        };
    }

    @Test(dataProvider = "aggregationInputs")
    public void expressionToAggregation(AbstractAggregationBuilder aggsBuilder, String json) throws Exception {
        assertAgg(aggsBuilder, json);
    }

    @Test
    public void complexTerm() throws Exception {
        Builder root = termBuilder("sources", "source");
        Builder entities = root.addAndGet(termBuilder("entities", "entity"));
        Builder traders = entities.addAndGet(termBuilder("traders", "trader"));
        traders.add(statsBuilder("traderPriceStats", "price"));
        Builder types = traders.addAndGet(dateHistogramBuilder("dates", "trade_date", "month"));
        types.add(statsBuilder("quantityStats", "quantity"));
        assertAgg(Term.buildFrom(root.build()), "{\"aggregations\":{\"sources\":{\"terms\":{\"field\":\"source\"," +
           "\"size\":20,\"show_term_doc_count_error\":true},\"aggregations\":{\"entities\":{\"terms\":{\"" +
           "field\":\"entity\",\"size\":20,\"show_term_doc_count_error\":true},\"aggregations\":{\"traders\"" +
           ":{\"terms\":{\"field\":\"trader\",\"size\":20,\"show_term_doc_count_error\":true},\"aggregations\"" +
           ":{\"traderPriceStats\":{\"stats\":{\"field\":\"price\"}},\"dates\":{\"date_histogram\":{\"field" +
           "\":\"trade_date\",\"interval\":\"month\",\"format\":\"yyyy-MM-dd\"},\"aggregations\":{" +
           "\"quantityStats\":{\"stats\":{\"field\":\"quantity\"}}}}}}}}}}}}");
    }

    private static void assertAgg(AbstractAggregationBuilder aggsBuilder, String json) throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.aggregation(aggsBuilder);
        assertEquals(convertToJson(builder.buildAsBytes(), false, false), json);
    }
}