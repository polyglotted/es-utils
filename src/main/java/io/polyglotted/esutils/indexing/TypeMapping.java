package io.polyglotted.esutils.indexing;

import com.google.common.collect.ImmutableSortedSet;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSortedSet.copyOf;

@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TypeMapping {
    public final String index;
    public final String type;
    public final boolean strict;
    public final boolean storeSource;
    public final ImmutableSortedSet<String> sourceIncludes;
    public final ImmutableSortedSet<FieldMapping> fieldMappings;
    //TODO transform

    public static Builder builder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String index;
        private String type;
        private boolean strict = false;
        private boolean storeSource = true;
        private final Set<String> sourceIncludes = new HashSet<>();
        private final Set<FieldMapping> fieldMappings = new HashSet<>();

        public Builder fieldMapping(FieldMapping.Builder mapping) {
            return fieldMapping(mapping.build());
        }

        public Builder fieldMapping(FieldMapping mapping) {
            fieldMappings.add(mapping);
            if (mapping.includeInSource) sourceIncludes.add(mapping.field);
            return this;
        }

        public TypeMapping build() {
            checkArgument(!fieldMappings.isEmpty(), "atleast one field must be indexed");
            return new TypeMapping(checkNotNull(index, "index cannot be null"), checkNotNull(type,
                    "type cannot be null"), strict, storeSource, copyOf(sourceIncludes), copyOf(fieldMappings));
        }
    }
}
