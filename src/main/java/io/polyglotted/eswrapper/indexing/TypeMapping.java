package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableMap.of;
import static io.polyglotted.eswrapper.ElasticConstants.ALL_META;
import static io.polyglotted.eswrapper.ElasticConstants.SOURCE_META;
import static io.polyglotted.eswrapper.ElasticConstants.TYPE_META;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class TypeMapping {
    public final String index;
    public final String type;
    public final String parent;
    public final boolean strict;
    public final boolean store;
    public final Boolean all;
    public final String analyzer;
    public final ImmutableSet<String> includes;
    public final ImmutableSet<FieldMapping> mappings;
    public final ImmutableList<TransformScript> scripts;
    public final ImmutableMap<String, Object> meta;

    @Override
    public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) &&
           mappingJson().equals(((TypeMapping) o).mappingJson()));
    }

    @Override
    public int hashCode() {
        return 29 * mappingJson().hashCode();
    }

    public String mappingJson() {
        return GSON.toJson(this);
    }

    public static String forcedMappingJson(String type) {
        return GSON.toJson(of(type, of(SOURCE_META, of("enabled", 0), ALL_META, of("enabled", 0),
           TYPE_META, of("index", "no"), "enabled", 0)));
    }

    public static Builder typeBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String index;
        private String type;
        private String parent;
        private boolean strict = false;
        private boolean storeSource = true;
        private Boolean allEnabled = true;
        private String allAnalyzer = "all_analyzer";
        private final Set<String> sourceIncludes = new TreeSet<>();
        private final Set<FieldMapping> fieldMappings = new TreeSet<>();
        private final List<TransformScript> transformScripts = new ArrayList<>();
        private final Map<String, Object> metaData = new LinkedHashMap<>();

        public Builder fieldMapping(FieldMapping.Builder mapping) {
            return fieldMapping(mapping.build());
        }

        public Builder fieldMapping(FieldMapping mapping) {
            fieldMappings.add(mapping);
            if (mapping.include) sourceIncludes.add(mapping.field);
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
            return new TypeMapping(checkNotNull(index, "index cannot be null"), checkNotNull(type, "type cannot be null"),
               parent, strict, storeSource, allEnabled, allAnalyzer, ImmutableSet.copyOf(sourceIncludes),
               ImmutableSet.copyOf(fieldMappings), ImmutableList.copyOf(transformScripts), ImmutableMap.copyOf(metaData));
        }
    }
}
