package io.polyglotted.eswrapper.query.response;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.indexing.IndexKey;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import static com.google.common.base.Optional.fromNullable;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class SimpleDoc {
    public final IndexKey key;
    public final ImmutableMap<String, Object> source;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleDoc other = (SimpleDoc) o;
        return key.equals(other.key) && source.equals(other.source);
    }

    @Override
    public int hashCode() {
        return 17 * key.hashCode() + source.hashCode();
    }

    public IndexKey key() {
        return key;
    }

    public long version() {
        return key.version;
    }

    public boolean boolVal(String property) {
        return fromNullable((Boolean) source.get(property)).or(false);
    }

    public int intVal(String property) {
        return fromNullable((Integer) source.get(property)).or(Integer.MIN_VALUE);
    }

    public long longVal(String property) {
        return fromNullable((Long) source.get(property)).or(Long.MIN_VALUE);
    }

    public long longStrVal(String property) {
        return Long.parseLong(strVal(property));
    }

    public String strVal(String property) {
        return (String) source.get(property);
    }

    public ActionRequest forcedRequest() {
        return new IndexRequest(key.index, key.type, key.id).version(key.version)
           .versionType(VersionType.FORCE).source(source);
    }
}