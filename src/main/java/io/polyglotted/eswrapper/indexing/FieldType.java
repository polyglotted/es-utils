package io.polyglotted.eswrapper.indexing;

import com.google.gson.JsonObject;

public enum FieldType {
    BOOLEAN,
    STRING,
    FLOAT,
    DOUBLE,
    BYTE,
    SHORT,
    INTEGER,
    LONG,
    DATE {
        @Override
        void extra(JsonObject object) {
            object.addProperty("format", "dateOptionalTime");
        }
    },
    BINARY,
    NESTED,
    IP,
    GEO_POINT,
    GEO_SHAPE {
        @Override
        void extra(JsonObject object) {
            object.addProperty("tree", "quadtree");
        }
    },
    OBJECT;

    void extra(JsonObject object) {}
}
