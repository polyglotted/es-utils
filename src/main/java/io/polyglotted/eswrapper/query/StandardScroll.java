package io.polyglotted.eswrapper.query;

import lombok.RequiredArgsConstructor;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@RequiredArgsConstructor
public final class StandardScroll {
    public final String id;
    public final long scroll;

    @Override
    public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) &&
           id.equals(((StandardScroll) o).id) && scroll == ((StandardScroll) o).scroll);
    }

    @Override
    public int hashCode() {
        return 31 * id.hashCode() + (int) (19 * scroll);
    }

    public static StandardScroll fromScrollId(String scrollId) {
        return new StandardScroll(scrollId, timeValueMinutes(5).millis());
    }
}
