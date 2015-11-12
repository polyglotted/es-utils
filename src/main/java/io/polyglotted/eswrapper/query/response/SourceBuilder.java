package io.polyglotted.eswrapper.query.response;

import java.util.Map;

public interface SourceBuilder<T> {

    T buildFrom(Map<String, ?> sourceMap);

    SourceBuilder<Map<String, ?>> DEFAULT_BUILDER = sourceMap -> sourceMap;
}
