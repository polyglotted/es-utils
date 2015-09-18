package io.polyglotted.esutils.indexing;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.search.SearchHit;

import static com.google.common.collect.ComparisonChain.start;

@ToString(includeFieldNames = false)
@RequiredArgsConstructor
public final class IndexKey implements Comparable<IndexKey> {
    public final String index;
    public final String type;
    public final String id;
    public final long version;

    public static IndexKey from(BulkItemResponse response) {
        return new IndexKey(response.getIndex(), response.getType(), response.getId(), response.getVersion());
    }

    public static IndexKey from(SearchHit searchHit) {
        return new IndexKey(searchHit.getIndex(), searchHit.getType(), searchHit.getId(), searchHit.getVersion());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexKey that = (IndexKey) o;
        return index.equals(that.index) && id.equals(that.id) && (version == that.version);
    }

    @Override
    public int hashCode() {
        return 17 * index.hashCode() + 31 * id.hashCode() + (int) version;
    }

    @Override
    public int compareTo(IndexKey other) {
        return other == null ? -1 : start().compare(index, other.index)
           .compare(id, other.id).compare(version, other.version).result();
    }
}
