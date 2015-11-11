package io.polyglotted.eswrapper.validation;

import java.util.Collection;

public interface Validator {

    Validity validate(Collection<String> updateIds);

    Validator EMPTY_VALIDATOR = (currentDocs) -> Validity.valid();
}
