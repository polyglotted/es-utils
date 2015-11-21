package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableList;
import io.polyglotted.eswrapper.query.request.Expression;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.cluster.metadata.AliasAction;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.toArray;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.query.ExpressionType.buildFilter;
import static java.util.Arrays.asList;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class Alias {
    public final String alias;
    public final ImmutableList<String> indices;
    public final Expression filter;
    public final boolean remove;

    public IndicesAliasesRequest.AliasActions action() {
        return new IndicesAliasesRequest.AliasActions(remove ? AliasAction.Type.REMOVE : AliasAction.Type.ADD,
           toArray(indices, String.class), new String[]{alias}).filter(buildFilter(filter));
    }

    public List<String> indices() {
        return indices;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass()) && GSON.toJson(this).equals(GSON.toJson(o));
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, indices, filter, remove);
    }

    public static Builder aliasBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String alias;
        private final Set<String> indices = new TreeSet<>();
        private Expression filter;
        private boolean remove = false;

        public Builder index(String... indices) {
            this.indices.addAll(asList(indices));
            return this;
        }

        public Builder remove() {
            this.remove(true);
            return this;
        }

        public Alias build() {
            checkArgument(!indices.isEmpty(), "atleast one index must be added");
            return new Alias(checkNotNull(alias, "alias required"), ImmutableList.copyOf(indices), filter, remove);
        }
    }
}
