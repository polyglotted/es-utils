package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class TransformScript {
    public final String script;
    public final ImmutableMap<String, Object> params;
    public final String lang;

    @Override
    public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) &&
           scriptJson().equals(((TransformScript) o).scriptJson()));
    }

    @Override
    public int hashCode() {
        return 31 * scriptJson().hashCode();
    }

    public String scriptJson() {
        return GSON.toJson(this);
    }

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
                    params.isEmpty() ? ImmutableMap.of() : ImmutableMap.copyOf(params), lang);
        }
    }
}
