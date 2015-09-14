package io.polyglotted.esutils.indexing;

import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.esutils.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor
public final class FieldMapping implements Comparable<FieldMapping> {
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
        return field.compareTo(other.field);
    }

    @SuppressWarnings("unused")
    public enum Indexed {
        NOT_ANALYZED, ANALYZED, NO
    }

    public static Builder builder() {
        return new Builder();
    }

    public static FieldMapping.Builder notAnalyzedStringField(String field) {
        return builder().field(field).type(FieldType.STRING).indexed(Indexed.NOT_ANALYZED).docValues(true);
    }

    public static FieldMapping privateField(String field) {
        return builder().field(field).type(FieldType.STRING).indexed(Indexed.NOT_ANALYZED).stored(null)
                .includeInSource(true).includeInAll(false).docValues(true).build();
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
