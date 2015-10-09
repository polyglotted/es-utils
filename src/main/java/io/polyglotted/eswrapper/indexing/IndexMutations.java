package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.*;
import java.util.Map.Entry;

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class IndexMutations<T> {
    public final ImmutableList<T> creates;
    public final ImmutableMap<IndexKey, T> updates;
    public final ImmutableList<IndexKey> deletes;

    public Indexable toIndexable(String index, long timestamp, Function<T, IndexRecord>
       creator, Function<Entry<IndexKey, T>, IndexRecord> updator) {

        return indexableBuilder().index(index).timestamp(timestamp)
           .records(transform(creates, creator))
           .records(transform(updates.entrySet(), updator))
           .records(transform(deletes, IndexRecord::deleteRecord))
           .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexMutations that = (IndexMutations) o;
        return creates.equals(that.creates) && updates.equals(that.updates) && deletes.equals(that.deletes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(creates, updates, deletes);
    }

    public static <T> Builder<T> mutationsBuilder() {
        return new Builder<>();
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder<T> {
        private final List<T> creates = new LinkedList<>();
        private final Map<IndexKey, T> updates = new LinkedHashMap<>();
        private final List<IndexKey> deletes = new LinkedList<>();

        public Builder<T> creates(List<T> objects) {
            this.creates.addAll(objects);
            return this;
        }

        public Builder<T> updates(Map<IndexKey, T> updates) {
            this.updates.putAll(updates);
            return this;
        }

        public Builder<T> deletes(List<IndexKey> keys) {
            this.deletes.addAll(keys);
            return this;
        }

        public IndexMutations<T> build() {
            return new IndexMutations<>(ImmutableList.copyOf(creates), ImmutableMap.copyOf(updates),
               ImmutableList.copyOf(deletes));
        }
    }
}
