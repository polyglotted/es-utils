package io.polyglotted.eswrapper.indexing;

import com.google.common.annotations.VisibleForTesting;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.search.SearchHit;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;

import static com.google.common.collect.ComparisonChain.start;
import static io.polyglotted.eswrapper.indexing.KeyUtil.generateUuid;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class IndexKey implements Comparable<IndexKey> {
    public final String index;
    public final String type;
    public final String id;
    public final long version;

    public String uniqueId() {
        return generateUuid(writeToStream(this, new ByteArrayOutputStream()).toByteArray()).toString();
    }

    public static IndexKey keyWith(String type, String id) {
        return new IndexKey("", type, id, -1);
    }

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

    @VisibleForTesting
    static <OS extends OutputStream> OS writeToStream(IndexKey indexKey, OS output) {
        try{
            DataOutputStream stream = new DataOutputStream(output);
            stream.writeBytes(indexKey.index);
            stream.writeBytes(indexKey.id);
            stream.writeLong(indexKey.version);
            stream.close();
        } catch(Exception ex) {
            throw new RuntimeException("failed to writeToStream");
        }
        return output;
    }
}
