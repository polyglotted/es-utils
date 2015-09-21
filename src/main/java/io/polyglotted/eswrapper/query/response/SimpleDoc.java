package io.polyglotted.eswrapper.query.response;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.indexing.IndexKey;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import static com.google.common.base.Optional.fromNullable;

@ToString(includeFieldNames = false)
@RequiredArgsConstructor
public final class SimpleDoc {
    public final IndexKey indexKey;
    public final ImmutableMap<String, Object> source;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleDoc other = (SimpleDoc) o;
        return indexKey.equals(other.indexKey) && source.equals(other.source);
    }

    @Override
    public int hashCode() {
        return 17 * indexKey.hashCode() + source.hashCode();
    }

    public IndexKey key() {
        return indexKey;
    }

    public long longVal(String property) {
        return fromNullable((Long) source.get(property)).or(Long.MIN_VALUE);
    }

    public String strVal(String property) {
        return (String) source.get(property);
    }

    public ActionRequest forcedRequest() {
        return new IndexRequest(indexKey.index, indexKey.type, indexKey.id).version(indexKey.version)
           .versionType(VersionType.FORCE).source(source);
    }
}