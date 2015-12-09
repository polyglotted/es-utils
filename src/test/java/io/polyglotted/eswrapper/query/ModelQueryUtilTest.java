package io.polyglotted.eswrapper.query;

import io.polyglotted.pgmodel.search.query.SearchOptions;
import org.testng.annotations.Test;

import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class ModelQueryUtilTest extends ModelQueryUtil {


    @Test
    public void testOptionsFail() throws Exception {
        assertNull(toOptions(null));
    }

    @Test
    public void testOptionsFrom() throws Exception {
        for (SearchOptions options : SearchOptions.values()) {
            assertNotNull(toOptions(options));
        }
        assertEquals(toOptions(SearchOptions.valueOf("LENIENT_EXPAND_OPEN")), lenientExpandOpen());
    }
}