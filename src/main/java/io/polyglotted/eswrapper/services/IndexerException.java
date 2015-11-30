package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.esmodel.api.IndexKey;

import java.util.Map;

@SuppressWarnings("serial")
public final class IndexerException extends RuntimeException {
    public final ImmutableMap<IndexKey, String> errorsMap;

    public IndexerException(Map<IndexKey, String> errorsMap) {
        super(buildFailureMessage(errorsMap));
        this.errorsMap = ImmutableMap.copyOf(errorsMap);
    }

    private static String buildFailureMessage(Map<IndexKey, String> errorsMap) {
        StringBuilder sb = new StringBuilder();
        sb.append("indexing failed:");
        for (Map.Entry<IndexKey, String> entry : errorsMap.entrySet())
            sb.append("\n[").append(entry.getKey()).append("]: ").append(entry.getValue());
        return sb.toString();
    }
}
