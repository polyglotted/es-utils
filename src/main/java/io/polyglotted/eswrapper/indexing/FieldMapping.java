package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.eswrapper.indexing.FieldType.BINARY;
import static io.polyglotted.eswrapper.indexing.FieldType.LONG;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class FieldMapping implements Comparable<FieldMapping> {
    public static final String BYTES_FIELD = "&bytes";
    public static final String EXPIRY_FIELD = "&expiry";
    public static final String STATUS_FIELD = "&status";
    static final List<FieldMapping> PRIVATE_FIELDS = ImmutableList.of(simpleField(BYTES_FIELD, BINARY).build(),
       simpleField(EXPIRY_FIELD, LONG).includeInAll(false).build(),
       notAnalyzedStringField(STATUS_FIELD).docValues(true).includeInAll(false).build());
    static final Map<String, Object> PATH_FIELDS = ImmutableMap.of("tree", ImmutableMap.of("type", "string",
       "analyzer", "path_analyzer"));

    public final String field;
    public final boolean include;
    public final ImmutableMap<String, Object> mapping;

    @SuppressWarnings("unchecked")
    public FieldMapping(String field, boolean include, String mappingJson) {
        this(field, include, (mappingJson == null) ? ImmutableMap.of() : ImmutableMap.copyOf(GSON.fromJson(mappingJson, Map.class)));
    }

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

    @SuppressWarnings("unused")
    public enum Indexed {
        NOT_ANALYZED, ANALYZED, NO
    }

    public static Builder fieldBuilder() {
        return new Builder();
    }

    public static FieldMapping.Builder nestedField(String field) {
        return simpleField(field, FieldType.NESTED);
    }

    public static FieldMapping.Builder notAnalyzedStringField(String field) {
        return notAnalyzedField(field, FieldType.STRING).docValues(true);
    }

    public static FieldMapping.Builder notAnalyzedField(String field, FieldType fieldType) {
        return simpleField(field, fieldType).indexed(Indexed.NOT_ANALYZED).stored(null);
    }

    public static FieldMapping.Builder simpleField(String field, FieldType fieldType) {
        return fieldBuilder().field(field).type(fieldType);
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        @Getter
        private String field;
        @Getter
        private FieldType type;
        private boolean includeInSource = false;
        @Getter
        private Indexed indexed = null;
        @Getter
        private String indexer;
        @Getter
        private Boolean stored = null;
        @Getter
        private Boolean includeInAll = null;
        @Getter
        private Boolean docValues = null;
        @Getter
        private boolean isAPath = false;
        @Getter
        private final Map<String, Builder> properties = new TreeMap<>();

        public Builder property(Iterable<Builder> properties) {
            this.properties.putAll(Maps.uniqueIndex(properties, Builder::field));
            return this;
        }

        public boolean hasProperties() {
            return properties.size() > 0;
        }

        public FieldMapping build() {
            return new FieldMapping(checkNotNull(field, "field cannot be null"), includeInSource, GSON.toJson(this));
        }
    }
}
