package io.polyglotted.eswrapper.query;

import io.polyglotted.esmodel.api.Expressions;
import org.elasticsearch.index.query.FilterBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.polyglotted.eswrapper.query.ExprConverter.*;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToJson;
import static org.testng.Assert.assertEquals;

public class ExpressionsTest extends Expressions {

    @DataProvider
    public static Object[][] expressionInputs() {
        return new Object[][]{
           {All.buildFrom(all()), "{\"match_all\":{}}"},
           {Ids.buildFrom(ids("ab", "cd")), "{\"ids\":{\"types\":[],\"values\":[\"ab\",\"cd\"]}}"},
           {Eq.buildFrom(equalsTo("hello", "world")), "{\"term\":{\"hello\":\"world\"}}"},
           {Gte.buildFrom(greaterThanEquals("hello", "world")),
              "{\"range\":{\"hello\":{\"from\":\"world\",\"to\":null,\"include_lower\":true,\"include_upper\":true}}}"},
           {Gt.buildFrom(greaterThan("hello", "world")),
              "{\"range\":{\"hello\":{\"from\":\"world\",\"to\":null,\"include_lower\":false,\"include_upper\":true}}}"},
           {Lte.buildFrom(lessThanEquals("hello", "world")),
              "{\"range\":{\"hello\":{\"from\":null,\"to\":\"world\",\"include_lower\":true,\"include_upper\":true}}}"},
           {Lt.buildFrom(lessThan("hello", "world")),
              "{\"range\":{\"hello\":{\"from\":null,\"to\":\"world\",\"include_lower\":true,\"include_upper\":false}}}"},
           {Prefix.buildFrom(prefix("hello", "world")), "{\"prefix\":{\"hello\":\"world\"}}"},
           {Ne.buildFrom(notEquals("hello", "world")), "{\"not\":{\"filter\":{\"term\":{\"hello\":\"world\"}}}}"},
           {In.buildFrom(in("hello", "foo", "bar")), "{\"terms\":{\"hello\":[\"foo\",\"bar\"]}}"},
           {In.buildFrom(in("hello", 25, 32)), "{\"terms\":{\"hello\":[25,32]}}"},
           {Between.buildFrom(between("hello", "foo", "bar")),
              "{\"range\":{\"hello\":{\"from\":\"foo\",\"to\":\"bar\",\"include_lower\":true,\"include_upper\":false}}}"},
           {Text.buildFrom(textAnywhere("hello")),
              "{\"query\":{\"match\":{\"_all\":{\"query\":\"hello\",\"type\":\"phrase_prefix\"}}}}"},
           {Text.buildFrom(textAnywhere("a", "hello")),
              "{\"query\":{\"match\":{\"a\":{\"query\":\"hello\",\"type\":\"phrase_prefix\"}}}}"},
           {Regex.buildFrom(regex("hello", "wor*")), "{\"regexp\":{\"hello\":\"wor*\"}}"},
           {Exists.buildFrom(exists("hello")), "{\"exists\":{\"field\":\"hello\"}}"},
           {Missing.buildFrom(missing("hello")),
              "{\"missing\":{\"field\":\"hello\",\"null_value\":true,\"existence\":true}}"},
           {Type.buildFrom(type("hello")), "{\"type\":{\"value\":\"hello\"}}"},
           {Json.buildFrom(json("{\"query\":{\"match_phrase\":{\"_all\":{\"query\":\"commodity\",\"slop\":20}}}}")),
              "{\"wrapper\":{\"filter\":\"eyJxdWVyeSI6eyJtYXRjaF9waHJhc2UiOnsiX2FsbCI6eyJxdWVyeSI6ImNvbW1vZGl0eSIsInNsb3AiOjIwfX19fQ==\"}}"},
           {And.buildFrom(and(equalsTo("hello", "world"))), "{\"and\":{\"filters\":[{\"term\":{\"hello\":\"world\"}}]}}"},
           {And.buildFrom(liveIndex()), "{\"and\":{\"filters\":[{\"missing\":{\"field\":\"&status\",\"null_value\":true," +
              "\"existence\":true}},{\"missing\":{\"field\":\"&expiry\",\"null_value\":true,\"existence\":true}}]}}"},
           {And.buildFrom(archiveIndex()), "{\"and\":{\"filters\":[{\"exists\":{\"field\":\"&status\"}}," +
              "{\"exists\":{\"field\":\"&expiry\"}}]}}"},
           {Or.buildFrom(or(equalsTo("hello", "world"))), "{\"or\":{\"filters\":[{\"term\":{\"hello\":\"world\"}}]}}"},
           {Not.buildFrom(not(equalsTo("hello", "world"))), "{\"not\":{\"filter\":{\"term\":{\"hello\":\"world\"}}}}"},
           {Nested.buildFrom(nested("foo.bar", equalsTo("hello", "world"))),
              "{\"nested\":{\"filter\":{\"term\":{\"hello\":\"world\"}},\"path\":\"foo.bar\"}}"},
           {HasParent.buildFrom(hasParent("foo.bar", equalsTo("hello", "world"))),
              "{\"has_parent\":{\"filter\":{\"term\":{\"hello\":\"world\"}},\"parent_type\":\"foo.bar\"}}"},
           {HasChild.buildFrom(hasChild("foo.bar", equalsTo("hello", "world"))),
              "{\"has_child\":{\"filter\":{\"term\":{\"hello\":\"world\"}},\"child_type\":\"foo.bar\"}}"},
        };
    }

    @Test(dataProvider = "expressionInputs")
    public void expressionToFilter(FilterBuilder filterBuilder, String json) throws Exception {
        assertEquals(convertToJson(filterBuilder.buildAsBytes(), false, false), json);
    }
}