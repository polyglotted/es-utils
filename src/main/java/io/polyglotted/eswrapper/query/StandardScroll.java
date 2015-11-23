package io.polyglotted.eswrapper.query;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Objects;

import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class StandardScroll {
    public final String scrollId;
    public final long scrollTimeInMillis;

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && GSON.toJson(this).equals(GSON.toJson(o));
    }

    @Override
    public int hashCode() {
        return Objects.hash(scrollId, scrollTimeInMillis);
    }

    public static StandardScroll fromScrollId(String scrollId) {
        return new StandardScroll(scrollId, timeValueMinutes(5).millis());
    }
}
