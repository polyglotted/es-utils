package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Lists.transform;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static java.util.Arrays.asList;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class IndexAdmin {
    public final ImmutableList<String> indices;
    public final IndexAction action;
    public final IndexSetting setting;
    public final ImmutableList<Alias> aliases;

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && GSON.toJson(this).equals(GSON.toJson(o));
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, action, setting, aliases);
    }

    @SuppressWarnings("unused")
    public enum IndexAction {
        CREATE_INDEX, DROP_INDEX, UPDATE_SETTING, UPDATE_ALIAS, FORCE_REFRESH
    }

    public static Builder adminBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final List<String> indices = new ArrayList<>();
        private IndexAction action = IndexAction.CREATE_INDEX;
        private IndexSetting setting;
        private final List<Alias> aliases = new ArrayList<>();

        public Builder index(String... indices) {
            this.indices.addAll(asList(indices));
            return this;
        }

        public Builder alias(Alias... aliases) {
            this.aliases.addAll(asList(aliases));
            return this;
        }

        public Builder alias(Alias.Builder... builders) {
            this.aliases.addAll(transform(asList(builders), Alias.Builder::build));
            return this;
        }

        public IndexAdmin build() {
            return new IndexAdmin(copyOf(indices), action, setting, copyOf(aliases));
        }
    }
}
