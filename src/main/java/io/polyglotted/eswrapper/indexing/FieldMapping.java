package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.polyglotted.eswrapper.indexing.FieldType.BINARY;
import static io.polyglotted.eswrapper.indexing.FieldType.DATE;
import static io.polyglotted.eswrapper.indexing.FieldType.NESTED;
import static io.polyglotted.eswrapper.indexing.FieldType.OBJECT;
import static io.polyglotted.eswrapper.indexing.FieldType.STRING;
import static io.polyglotted.eswrapper.indexing.Indexed.NOT_ANALYZED;
import static java.util.Collections.singleton;

@RequiredArgsConstructor
@Accessors(fluent = true)
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class FieldMapping implements Comparable<FieldMapping> {
    public static final String BYTES_FIELD = "&bytes";
    public static final String TIMESTAMP_FIELD = "&timestamp";
    public static final String EXPIRY_FIELD = "&expiry";
    public static final String ANCESTOR_FIELD = "&ancestor";
    public static final String STATUS_FIELD = "&status";

    static final List<FieldMapping> PRIVATE_FIELDS = ImmutableList.of(
       notAnalyzedStringField(ANCESTOR_FIELD).docValues(true).includeInAll(false).build(),
       simpleField(BYTES_FIELD, BINARY).build(),
       simpleField(EXPIRY_FIELD, DATE).includeInAll(false).build(),
       notAnalyzedStringField(STATUS_FIELD).docValues(true).includeInAll(false).build(),
       simpleField(TIMESTAMP_FIELD, DATE).includeInAll(false).build());

    @Getter
    public final String field;
    public final FieldType type;
    public final Indexed indexed;
    public final String analyzer;
    public final String copyTo;
    public final Boolean docValues;
    public final Boolean includeInAll;
    public final Boolean hasFields;
    public final Boolean stored;
    public final ImmutableMap<String, Object> argsMap;
    public final ImmutableMap<String, FieldMapping> properties;

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && field.equals(((FieldMapping) o).field);
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }

    @Override
    public int compareTo(FieldMapping other) {
        return (other == null) ? -1 : field.compareTo(other.field);
    }

    public boolean hasProperties() {
        return (type == NESTED || type == OBJECT) && properties.size() > 0;
    }

    public boolean hasFields() {
        return Boolean.TRUE.equals(hasFields) && properties.size() > 0;
    }

    public static Builder fieldBuilder() {
        return new Builder();
    }

    public static FieldMapping.Builder objectField(String field) {
        return simpleField(field, OBJECT);
    }

    public static FieldMapping.Builder nestedField(String field) {
        return simpleField(field, NESTED);
    }

    public static FieldMapping.Builder notAnalyzedStringField(String field) {
        return notAnalyzedField(field, STRING).docValues(true);
    }

    public static FieldMapping.Builder notAnalyzedField(String field, FieldType fieldType) {
        return simpleField(field, fieldType).indexed(NOT_ANALYZED);
    }

    public static FieldMapping.Builder simpleField(String field, FieldType fieldType) {
        return fieldBuilder().field(field).type(fieldType);
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String field;
        private FieldType type;
        private Indexed indexed = null;
        private String analyzer = null;
        private String copyTo = null;
        private Boolean docValues = null;
        private Boolean includeInAll = null;
        private Boolean hasFields = null;
        private Boolean stored = null;
        private final Map<String, Object> args = new TreeMap<>();
        private final Map<String, FieldMapping> properties = new TreeMap<>();

        public Builder property(Iterable<Builder> properties) {
            this.properties.putAll(uniqueIndex(transform(properties, Builder::build), FieldMapping::field));
            return this;
        }

        public Builder isAPath() {
            hasFields(true);
            return property(singleton(simpleField("tree", STRING).analyzer("path_analyzer")));
        }

        public Builder addRawFields() {
            hasFields(true);
            return property(singleton(notAnalyzedStringField("raw")));
        }

        public Builder extra(String name, Object value) {
            args.put(name, value);
            return this;
        }

        public FieldMapping build() {
            return new FieldMapping(checkNotNull(field, "field is required"), checkNotNull(type, "type is required"),
               indexed, analyzer, copyTo, docValues, includeInAll, hasFields, stored, copyOf(args), copyOf(properties));
        }
    }
}
