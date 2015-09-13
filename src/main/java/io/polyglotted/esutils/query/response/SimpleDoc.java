package io.polyglotted.esutils.query.response;

import lombok.RequiredArgsConstructor;

import java.sql.Time;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Optional.fromNullable;

@RequiredArgsConstructor
public final class SimpleDoc {
    public final String index;
    public final String type;
    public final String id;
    public final Map<String, Object> source;

    public <T extends Enum<T>> T enumVal(Class<T> enumClass, String property) {
        String name = strVal(property);
        return name == null ? null : Enum.valueOf(enumClass, name);
    }

    public Time timeVal(String property) {
        return new Time(longVal(property));
    }

    public UUID uuidVal(String property) {
        return fromNullable(strVal(property)).transform(UUID::fromString).or(new UUID(0, 0));
    }

    public long longVal(String property) {
        return fromNullable((Long) source.get(property)).or(Long.MIN_VALUE);
    }

    public String strVal(String property) {
        return (String) source.get(property);
    }
}