package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.indexing.IndexRecord.Builder;
import io.polyglotted.eswrapper.services.Portfolio;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.updateRecord;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.pgmodel.search.IndexKey.keyWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class IndexRecordTest {

    @DataProvider
    public static Object[][] recordInputs() {
        return new Object[][]{
           {new Portfolio("/p/1", "first portfolio"), createRecord("a", "aa", ""), Portfolio.class,
              "{\"address\":\"/p/1\",\"name\":\"first portfolio\",\"&baseKey\":\"_auto_\",\"&timestamp\":" +
                 "\"1425494500000\",\"&user\":\"tester\"}", null},
           {new Portfolio("/p/1", "first portfolio"), createRecord("a", "aa", "c"), Portfolio.class,
              "{\"address\":\"/p/1\",\"name\":\"first portfolio\",\"&baseKey\":\"c\",\"&timestamp\":" +
                 "\"1425494500000\",\"&user\":\"tester\"}", null},
           {new Portfolio("/p/1", "first portfolio"), updateRecord(keyWith("a", "aa", "c")), Portfolio.class,
              "{\"address\":\"/p/1\",\"name\":\"first portfolio\",\"&ancestor\":" +
                 "\"1239fff0-ff8e-5244-b1b9-133119b1b4d1\",\"&baseKey\":\"c\",\"&timestamp\":\"1425494500000\"," +
                 "\"&user\":\"tester\"}", null},
           {ImmutableMap.of(), createRecord("a", "aa", "c"), Map.class,
              "{\"&baseKey\":\"c\",\"&timestamp\":\"1425494500000\",\"&user\":\"tester\"}",
              ImmutableMap.of("&baseKey", "c", "&timestamp", "1425494500000", "&user", "tester")},
           {ImmutableMap.of(), updateRecord(keyWith("a", "aa", "c")), Map.class,
              "{\"&ancestor\":\"1239fff0-ff8e-5244-b1b9-133119b1b4d1\",\"&baseKey\":\"c\",\"&timestamp\":" +
                 "\"1425494500000\",\"&user\":\"tester\"}", ImmutableMap.of("&ancestor",
              "1239fff0-ff8e-5244-b1b9-133119b1b4d1", "&baseKey", "c", "&timestamp", "1425494500000", "&user", "tester")},
        };
    }

    @Test(dataProvider = "recordInputs")
    public void testSourceOf(Object original, Builder builder, Class<?>
       clazz, String message, Object expected) throws Exception {
        IndexRecord record = builder.source(GSON.toJson(original)).build();

        String source = RecordAction.sourceOf(record, 1425494500000L, "tester");
        assertThat(source, source, is(message));

        Object actual = GSON.fromJson(source, clazz);
        assertThat(source, actual, is(expected == null ? original : expected));
    }
}