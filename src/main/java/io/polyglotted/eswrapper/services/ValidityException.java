package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class ValidityException extends IndexerException {

    protected ValidityException(ImmutableMap<String, Object> errorsMap) {
        super(errorsMap);
    }

    public static <K, V> void checkValidity(Map<K, V>  validations) {
        if (!validations.isEmpty()) {
            throw new ValidityException(ImmutableMap.of("ERROR_MESSAGE",
               buildFailureMessage("validation failed:", validations), "ERRORS", validations));
        }
    }
}
