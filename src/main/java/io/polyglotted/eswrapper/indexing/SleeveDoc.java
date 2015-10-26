package io.polyglotted.eswrapper.indexing;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Objects;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class SleeveDoc {
    public final IndexKey key;
    public final Object source;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SleeveDoc other = (SleeveDoc) o;
        return key.equals(other.key) && source.equals(other.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, source);
    }
}
