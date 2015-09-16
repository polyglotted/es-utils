package io.polyglotted.esutils.indexing;

import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@RequiredArgsConstructor
public final class TransformScript {
    public final String script;
    public final ImmutableMap<String, Object> params;
    public final String lang;

    public static Builder transformBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String script;
        private String lang;
        private final Map<String, Object> params = new HashMap<>();

        public Builder param(String name, Object value) {
            params.put(name, value);
            return this;
        }

        public TransformScript build() {
            return new TransformScript(checkNotNull(script, "script cannot be null"),
                    params.isEmpty() ? null : ImmutableMap.copyOf(params), lang);
        }
    }
}
