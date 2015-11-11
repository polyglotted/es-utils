package io.polyglotted.eswrapper.validation;

import lombok.RequiredArgsConstructor;

@SuppressWarnings("serial")
@RequiredArgsConstructor
public class ValidException extends RuntimeException {
    public final Validity validity;

    public static void checkValidity(Validity validity) {
        if (!validity.isValid()) throw new ValidException(validity);
    }
}
