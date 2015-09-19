package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.or;
import static com.google.common.collect.Maps.filterKeys;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor
public final class IndexSetting {
    private static final String DEFAULT_ANALYSIS = "{\"analysis\":{\"analyzer\":{\"default\":" +
       "{\"type\":\"keyword\"},\"all_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"whitespace\"," +
       "\"filter\":[\"lowercase\"]}}},";
    public final ImmutableMap<String, Object> map;

    public String createJson() {
        return DEFAULT_ANALYSIS + GSON.toJson(map).substring(1);
    }

    public String updateJson() {
        return GSON.toJson(filterKeys(map, not(or("number_of_shards"::equals, "mapping.ignore_malformed"::equals))));
    }

    public static IndexSetting with(int numberOfShards, int numberOfReplicas) {
        return settingBuilder().numberOfShards(numberOfShards).numberOfReplicas(numberOfReplicas)
           .ignoreMalformed().build();
    }

    public static Builder settingBuilder() {
        return new Builder();
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final Map<String, Object> map = new LinkedHashMap<>();

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

        public Builder any(String name, Object value) {
            map.put(name, value);
            return this;
        }

        public IndexSetting build() {
            return new IndexSetting(ImmutableMap.copyOf(map));
        }
    }
}
