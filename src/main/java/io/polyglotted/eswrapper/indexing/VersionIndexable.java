package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformEntries;
import static io.polyglotted.eswrapper.indexing.IndexKey.keyWith;

@RequiredArgsConstructor
public final class VersionIndexable {
    public final String currentIndex;
    public final String historyIndex;
    private final ImmutableMap<IndexKey, String> writes;
    private final ImmutableMap<IndexKey, String> updates;
    private final long timestamp;

    public String[] indices() { return new String[]{currentIndex, historyIndex}; }

    public String[] currentIds() {
        return toArray(transform(writes.keySet(), IndexKey::id), String.class);
    }

    public Collection<IndexKey> updateKeys() {
        return updates.keySet();
    }

    public BulkRequest updateRequest() {
        Collection<ActionRequest> values = transformEntries(updates, (indexKey, source) -> (ActionRequest)
           new IndexRequest(historyIndex, indexKey.type, indexKey.id).create(true).source(source)
              .versionType(VersionType.EXTERNAL).version(timestamp)).values();
        return new BulkRequest().refresh(false).add(values);
    }

    public BulkRequest writeRequest() {
        Collection<ActionRequest> values = transformEntries(writes, (indexKey, source) -> (ActionRequest)
           new IndexRequest(currentIndex, indexKey.type, indexKey.id).source(source)
              .versionType(VersionType.EXTERNAL_GTE).version(timestamp)).values();
        return new BulkRequest().refresh(false).add(values);
    }

    public static Builder indexableBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String currentIndex;
        private String historyIndex;
        private String defaultType;
        private long timestamp = System.currentTimeMillis();
        private final Map<IndexKey, String> writes = new LinkedHashMap<>();
        private final Map<IndexKey, String> updates = new LinkedHashMap<>();

        public Builder write(String id, String json) {
            return write(defaultType, id, json);
        }

        public Builder write(String type, String id, String json) {
            writes.put(keyWith(checkNotNull(type), checkNotNull(id)), checkNotNull(json));
            return this;
        }

        public VersionIndexable build() {
            return new VersionIndexable(checkNotNull(currentIndex, "current index cannot be null"),
               checkNotNull(historyIndex, "history index cannot be null"), ImmutableMap.copyOf(writes),
               ImmutableMap.copyOf(updates), timestamp);
        }
    }
}
