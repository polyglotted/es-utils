package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.eswrapper.indexing.FieldType.LONG;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor
public final class FieldMapping implements Comparable<FieldMapping> {
    public static final String STATUS_FIELD = "&status";
    public static final String EXPIRY_FIELD = "&expiry";
    static final List<FieldMapping> PRIVATE_FIELDS = ImmutableList.of(notAnalyzedStringField(STATUS_FIELD)
       .docValues(true).includeInAll(false).build(), notAnalyzedField(EXPIRY_FIELD, LONG).includeInAll(false).build());

    public final String field;
    public final boolean includeInSource;
    public final ImmutableMap<String, Object> mapping;

    @SuppressWarnings("unchecked")
    public FieldMapping(String field, boolean includeInSource, String mappingJson) {
        this(field, includeInSource, ImmutableMap.copyOf(GSON.fromJson(mappingJson, Map.class)));
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

    public static FieldMapping.Builder notAnalyzedStringField(String field) {
        return notAnalyzedField(field, FieldType.STRING).docValues(true);
    }

    public static FieldMapping.Builder notAnalyzedField(String field, FieldType fieldType) {
        return fieldBuilder().field(field).type(fieldType).indexed(Indexed.NOT_ANALYZED).stored(null);
    }

    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        @Setter
        private String field;
        @Getter
        private FieldType type;
        @Setter
        private boolean includeInSource = false;
        @Getter
        @Setter
        private Indexed indexed = null;
        @Getter
        @Setter
        private Boolean stored = null;
        @Getter
        @Setter
        private Boolean includeInAll = null;
        @Getter
        @Setter
        private Boolean docValues = null;

        public Builder type(FieldType fieldType) {
            this.type = fieldType;
            return this;
        }

        public FieldMapping build() {
            return new FieldMapping(checkNotNull(field, "field cannot be null"), includeInSource, GSON.toJson(this));
        }
    }
}
