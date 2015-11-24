package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.or;
import static com.google.common.collect.Maps.filterKeys;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class IndexSetting {
    public final ImmutableMap<String, Object> map;

    private static final Predicate<String> SHARDS_PREDICATE = "number_of_shards"::equals;
    private static final Predicate<String> MAPPING_PREDICATE = input -> input.startsWith("mapp");
    private static final Predicate<String> ANALYSIS_PREDICATE = input -> input.startsWith("analysis");

    @Override
    public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) && map.equals(((IndexSetting) o).map));
    }

    @Override
    public int hashCode() {
        return 19 * map.hashCode();
    }

    public String createJson() {
        return GSON.toJson(map);
    }

    @SuppressWarnings("unchecked")
    public String updateJson() {
        return GSON.toJson(filterKeys(map, not(or(SHARDS_PREDICATE, MAPPING_PREDICATE, ANALYSIS_PREDICATE))));
    }

    public static IndexSetting with(int numberOfShards, int numberOfReplicas) {
        return settingBuilder(numberOfShards, numberOfReplicas).build();
    }

    public static IndexSetting.Builder settingBuilder(int numberOfShards, int numberOfReplicas) {
        return settingBuilder().defaultAnalyzers().numberOfShards(numberOfShards)
           .numberOfReplicas(numberOfReplicas).ignoreMalformed();
    }

    public static Builder settingBuilder() {
        return new Builder();
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final Map<String, Object> map = new TreeMap<>();

        public Builder numberOfShards(int numberOfShards) {
            map.put("number_of_shards", numberOfShards);
            return this;
        }

        public Builder numberOfReplicas(int numberOfReplicas) {
            map.put("number_of_replicas", numberOfReplicas);
            return this;
        }

        public Builder refreshInterval(long refreshInterval) {
            map.put("refresh_interval", refreshInterval);
            return this;
        }

        public Builder ignoreMalformed() {
            map.putIfAbsent("mapping.ignore_malformed", true);
            return this;
        }

        public Builder disableDynamicMapping() {
            map.putIfAbsent("mapper.dynamic", false);
            return this;
        }

        public Builder autoExpandReplicas() {
            map.putIfAbsent("auto_expand_replicas", "0-all");
            return this;
        }

        public Builder any(String name, Object value) {
            map.put(name, value);
            return this;
        }

        public Builder defaultAnalyzers() {
            map.put("analysis", GSON.fromJson("{\"analyzer\":{\"default\":{\"type\":\"standard\"},\"all_analyzer\":" +
               "{\"type\":\"custom\",\"tokenizer\":\"whitespace\",\"filter\":[\"lowercase\"]},\"path_analyzer\":" +
               "{\"tokenizer\":\"path_hierarchy\",\"filter\":[\"lowercase\"]}}}", Map.class));
            return this;
        }

        public IndexSetting build() {
            return new IndexSetting(ImmutableMap.copyOf(map));
        }
    }
}
