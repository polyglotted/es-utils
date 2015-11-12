package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.indexing.IndexRecord.Builder;
import io.polyglotted.eswrapper.services.Portfolio;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.sourceOf;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class IndexRecordTest {

    @DataProvider
    public static Object[][] recordInputs() {
        return new Object[][]{
           {new Portfolio("/p/1", "first portfolio"), createRecord("a", "aa"), Portfolio.class,
              "{\"address\":\"/p/1\",\"name\":\"first portfolio\",\"&timestamp\":1425494500000}", null},
           {new Portfolio("/p/1", "first portfolio"), createRecord("a", "aa").ancestor("b"), Portfolio.class,
              "{\"address\":\"/p/1\",\"name\":\"first portfolio\",\"&timestamp\":1425494500000,\"&ancestor\":\"b\"}", null},
           {ImmutableMap.of(), createRecord("a", "aa"), Map.class,
              "{\"&timestamp\":1425494500000}", ImmutableMap.of("&timestamp", (double) 1425494500000L)},
           {ImmutableMap.of(), createRecord("a", "aa").ancestor("b"), Map.class,
              "{\"&timestamp\":1425494500000,\"&ancestor\":\"b\"}",
              ImmutableMap.of("&timestamp", (double) 1425494500000L, "&ancestor", "b")},
        };
    }

    @Test(dataProvider = "recordInputs")
    public void testSourceOf(Object original, Builder builder, Class<?>
       clazz, String message, Object expected) throws Exception {
        IndexRecord record = builder.source(GSON.toJson(original)).build();
        String source = sourceOf(record, 1425494500000L);
        assertThat(source, source, is(message));

        Object actual = GSON.fromJson(source, clazz);
        assertThat(source, actual, is(expected == null ? original : expected));
    }
}