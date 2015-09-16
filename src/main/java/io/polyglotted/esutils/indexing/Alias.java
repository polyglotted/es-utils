package io.polyglotted.esutils.indexing;

import io.polyglotted.esutils.query.request.Expression;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.cluster.metadata.AliasAction;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.toArray;
import static io.polyglotted.esutils.query.ExpressionType.buildFilter;
import static java.util.Arrays.asList;

@RequiredArgsConstructor
public final class Alias {
    public final String[] aliases;
    public final String[] indices;
    public final Expression filter;
    public final boolean remove;

    public IndicesAliasesRequest.AliasActions action() {
        return new IndicesAliasesRequest.AliasActions(remove ? AliasAction.Type.REMOVE : AliasAction.Type.ADD,
           indices, aliases).filter(buildFilter(filter));
    }

    public static Builder aliasBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private List<String> aliases = new ArrayList<>();
        private List<String> indices = new ArrayList<>();
        private Expression filter;
        private boolean remove = false;

        public Builder alias(String... aliases) {
            this.aliases.addAll(asList(aliases));
            return this;
        }

        public Builder index(String... indices) {
            this.indices.addAll(asList(indices));
            return this;
        }

        public Builder remove() {
            this.remove = true;
            return this;
        }

        public Alias build() {
            checkArgument(!aliases.isEmpty(), "atleast one alias must be added");
            checkArgument(!indices.isEmpty(), "atleast one index must be added");
            return new Alias(toArray(aliases, String.class), toArray(indices, String.class), filter, remove);
        }
    }
}
