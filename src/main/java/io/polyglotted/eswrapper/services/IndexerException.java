package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public class IndexerException extends RuntimeException {
    public final ImmutableMap<String, Object> errorsMap;

    protected IndexerException(ImmutableMap<String, Object> errorsMap) {
        super((String) errorsMap.get("ERROR_MESSAGE"));
        this.errorsMap = errorsMap;
    }

    public static <K, V> void checkErrors(Map<K, V> errorsMap) {
        if (!errorsMap.isEmpty()) {
            throw new IndexerException(ImmutableMap.of("ERROR_MESSAGE",
               buildFailureMessage("indexing failed:", errorsMap), "ERRORS", errorsMap));
        }
    }

    static <K, V> String buildFailureMessage(String message, Map<K, V>  errorsMap) {
        StringBuilder sb = new StringBuilder();
        sb.append(message);
        for (Entry<K, V> entry : errorsMap.entrySet())
            sb.append("\n[").append(entry.getKey()).append("]: ").append(entry.getValue());
        return sb.toString();
    }
}
