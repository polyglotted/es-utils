package io.polyglotted.eswrapper.query;

import lombok.RequiredArgsConstructor;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@RequiredArgsConstructor
public final class StandardScroll {
    public final String id;
    public final long scroll;

    public static StandardScroll fromScrollId(String scrollId) {
        return new StandardScroll(scrollId, timeValueMinutes(5).millis());
    }
}
