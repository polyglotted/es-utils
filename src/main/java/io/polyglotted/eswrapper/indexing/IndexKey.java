package io.polyglotted.eswrapper.indexing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.util.Map;

import static com.google.common.collect.ComparisonChain.start;
import static io.polyglotted.eswrapper.indexing.FieldMapping.STATUS_FIELD;
import static io.polyglotted.eswrapper.indexing.IndexRecord.Action.DELETE;
import static io.polyglotted.eswrapper.indexing.KeyUtil.generateUuid;
import static java.util.UUID.randomUUID;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class IndexKey implements Comparable<IndexKey> {
    public final String index;
    public final String type;
    public final String id;
    public final long version;
    public final boolean delete;
    public final String parent;

    public IndexKey(String index, String type, String id, long version) {
        this(index, type, id, version, false, null);
    }

    public String uniqueId() {
        return generateUuid(writeToStream(this, new ByteArrayOutputStream()).toByteArray()).toString();
    }

    public IndexKey delete() {
        return new IndexKey(index, type, id, version, true, null);
    }

    public IndexKey version(long version) {
        return new IndexKey(index, type, id, version, false, null);
    }

    public static IndexKey keyWith(String type, String id) {
        return keyWithParent("", type, id, null);
    }

    public static IndexKey keyWith(String index, String type, String id) {
        return keyWithParent(index, type, id, null);
    }

    public static IndexKey keyWithParent(String type, String parent) {
        return keyWithParent("", type, randomUUID().toString(), parent);
    }

    public static IndexKey keyWithParent(String type, String id, String parent) {
        return keyWithParent("", type, id, parent);
    }

    public static IndexKey keyWithParent(String index, String type, String id, String parent) {
        return new IndexKey(index, type, id, -1, false, parent);
    }

    public static IndexKey from(IndexResponse response) {
        return new IndexKey(response.getIndex(), response.getType(), response.getId(), response.getVersion());
    }

    public static IndexKey from(BulkItemResponse response) {
        return new IndexKey(response.getIndex(), response.getType(), response.getId(), response.getVersion());
    }

    public static IndexKey from(SearchHit searchHit) {
        return new IndexKey(searchHit.getIndex(), searchHit.getType(), searchHit.getId(),
           searchHit.getVersion(), isDeleted(searchHit), getParent(searchHit));
    }

    private static boolean isDeleted(SearchHit searchHit) {
        Map<String, Object> source = searchHit.isSourceEmpty() ? ImmutableMap.of() : searchHit.sourceAsMap();
        return DELETE.status.equals(source.get(STATUS_FIELD));
    }

    private static String getParent(SearchHit searchHit) {
        SearchHitField field = searchHit.field("_parent");
        return field == null ? null : (String) field.value();
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
        try {
            DataOutputStream stream = new DataOutputStream(output);
            stream.writeBytes(indexKey.index);
            stream.writeBytes(indexKey.id);
            stream.writeLong(indexKey.version);
            stream.close();
        } catch (Exception ex) {
            throw new RuntimeException("failed to writeToStream");
        }
        return output;
    }
}
