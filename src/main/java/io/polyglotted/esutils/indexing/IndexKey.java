package io.polyglotted.esutils.indexing;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import static com.google.common.collect.ComparisonChain.start;

@ToString(includeFieldNames = false)
@RequiredArgsConstructor
public final class IndexKey implements Comparable<IndexKey> {
    public final String index;
    public final String type;
    public final String id;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexKey that = (IndexKey) o;
        return index.equals(that.index) && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return 17 * index.hashCode() + id.hashCode();
    }

    @Override
    public int compareTo(IndexKey other) {
        return other == null ? -1 : start().compare(index, other.index).compare(id, other.id).result();
    }
}
