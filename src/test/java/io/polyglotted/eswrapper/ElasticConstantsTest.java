package io.polyglotted.eswrapper;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ElasticConstantsTest extends ElasticConstants {

    @Test
    public void assertConstants() {
        assertEquals(INDEX_META, "_index");
        assertEquals(ID_META, "_id");
        assertEquals(VERSION_META, "_version");
        assertEquals(FIELD_NAMES_META, "_field_names");
    }
}