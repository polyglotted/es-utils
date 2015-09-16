package io.polyglotted.esutils.indexing;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class IndexerException extends RuntimeException {
    private final ImmutableMap<IndexKey, String> errorsMap;

    public IndexerException(Map<IndexKey, String> errorsMap) {
        super("indexing failed: " + errorsMap);
        this.errorsMap = ImmutableMap.copyOf(errorsMap);
    }
}
