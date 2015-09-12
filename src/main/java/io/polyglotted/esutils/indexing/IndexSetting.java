package io.polyglotted.esutils.indexing;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class IndexSetting {
    public final int numberOfShards;
    public final int numberOfReplicas;
    public final Long refreshInterval;
    public final Boolean disableFlush;

    public static IndexSetting defaultSetting() {
        return with(5, 1);
    }

    public static IndexSetting with(int numberOfShards, int numberOfReplicas) {
        return new IndexSetting(numberOfShards, numberOfReplicas, null, null);
    }
}
