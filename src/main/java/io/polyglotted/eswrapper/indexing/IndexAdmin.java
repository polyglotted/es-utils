package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

@RequiredArgsConstructor
public final class IndexAdmin {
    public final String index;
    public final IndexAction action;
    public final IndexSetting setting;
    public final ImmutableList<Alias> aliases;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexAdmin that = (IndexAdmin) o;
        return index.equals(that.index) && action.equals(that.action) && aliases.equals(that.aliases) &&
           (setting == null ? that.setting==null : setting.equals(that.setting));
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, action, setting, aliases);
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
        private String index;
        private IndexAction action = IndexAction.CREATE_INDEX;
        private IndexSetting setting;
        private final List<Alias> aliases = new ArrayList<>();

        public Builder alias(Alias... aliases) {
            this.aliases.addAll(Arrays.asList(aliases));
            return this;
        }

        public IndexAdmin build() {
            return new IndexAdmin(checkNotNull(index, "index cannot be null"), action, setting,
               ImmutableList.copyOf(aliases));
        }
    }
}
