package io.polyglotted.esutils.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.*;

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
    public final ImmutableList<TransformScript> transformScripts;
    public final ImmutableMap<String, Object> metaData;

    public static Builder typeBuilder() {
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
        private final List<TransformScript> transformScripts = new ArrayList<>();
        private final Map<String, Object> metaData = new HashMap<>();

        public Builder fieldMapping(FieldMapping.Builder mapping) {
            return fieldMapping(mapping.build());
        }

        public Builder fieldMapping(FieldMapping mapping) {
            fieldMappings.add(mapping);
            if (mapping.includeInSource) sourceIncludes.add(mapping.field);
            return this;
        }

        public Builder transform(TransformScript.Builder script) {
            return transform(script.build());
        }

        public Builder transform(TransformScript script) {
            transformScripts.add(script);
            return this;
        }

        public Builder metaData(String name, Object value) {
            metaData.put(name, value);
            return this;
        }

        public TypeMapping build() {
            checkArgument(!fieldMappings.isEmpty(), "atleast one field must be indexed");
            return new TypeMapping(checkNotNull(index, "index cannot be null"), checkNotNull(type,
                    "type cannot be null"), strict, storeSource, copyOf(sourceIncludes), copyOf(fieldMappings),
                    ImmutableList.copyOf(transformScripts), ImmutableMap.copyOf(metaData));
        }
    }
}
