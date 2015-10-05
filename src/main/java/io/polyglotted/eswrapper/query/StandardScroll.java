package io.polyglotted.eswrapper.query;

import lombok.RequiredArgsConstructor;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@RequiredArgsConstructor
public final class StandardScroll {
    public final String scrollId;
    public final long scrollTimeInMillis;

    public static StandardScroll fromScrollId(String scrollId) {
        return new StandardScroll(scrollId, timeValueMinutes(5).millis());
    }
}
