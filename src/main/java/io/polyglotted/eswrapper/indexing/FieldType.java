package io.polyglotted.eswrapper.indexing;

import static io.polyglotted.eswrapper.indexing.FieldMapping.Indexed.NOT_ANALYZED;

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
        void decorateField(FieldMapping.Builder field) {
            super.decorateField(field);
            field.extra("format", "dateOptionalTime");
        }
    },
    BINARY,
    NESTED,
    IP,
    GEO_POINT,
    GEO_SHAPE,
    OBJECT;

    void decorateField(FieldMapping.Builder field) {
        if (field.type() != STRING && field.indexed() == NOT_ANALYZED) {
            field.indexed(null);
        }
    }
}
