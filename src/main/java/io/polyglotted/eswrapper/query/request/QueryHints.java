package io.polyglotted.eswrapper.query.request;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class QueryHints {
    public final String indicesOptions;
    public final String searchType;
    public final int timeoutInSeconds;
    public final ImmutableList<String> routing;
    public final String preference;
    public final boolean fetchSource;

    @SuppressWarnings("unused")
    public enum Options {
        strictExpandOpen, strictExpandOpenAndForbidClosed, strictExpand,
        strictSingleIndexNoExpandForbidClosed, lenientExpandOpen
    }

    @SuppressWarnings("unused")
    public enum SearchType {
        dfs_query_then_fetch, dfs_query_and_fetch,
        query_then_fetch, query_and_fetch, scan, count
    }

    public static Builder hintsBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Options indicesOptions = Options.lenientExpandOpen;
        private SearchType searchType = SearchType.query_then_fetch;
        private int timeoutInSeconds = 10;
        private final List<String> routing = new ArrayList<>();
        private String preference = null;
        private boolean fetchSource = true;

        public Builder routing(String... routing) {
            this.routing.addAll(asList(routing));
            return this;
        }

        public QueryHints build() {
            return new QueryHints(indicesOptions.name(), searchType.name(),
                    timeoutInSeconds, ImmutableList.copyOf(routing), preference, fetchSource);
        }
    }
}
