package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class IndexerException extends RuntimeException {
    private final ImmutableMap<IndexKey, String> errorsMap;

    public IndexerException(Map<IndexKey, String> errorsMap) {
        super(buildFailureMessage(errorsMap));
        this.errorsMap = ImmutableMap.copyOf(errorsMap);
    }

    private static String buildFailureMessage(Map<IndexKey, String> errorsMap) {
        StringBuilder sb = new StringBuilder();
        sb.append("indexing failed:");
        for (Map.Entry<IndexKey, String> entry : errorsMap.entrySet()) {
            sb.append("\n[").append(entry.getKey()).append("]: message [")
               .append(entry.getValue()).append("]");
        }
        return sb.toString();
    }
}
