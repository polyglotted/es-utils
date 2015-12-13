package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;

public interface Validator {

    Map<String, String> validate(Collection<String> updateIds);

    Validator EMPTY_VALIDATOR = (currentDocs) -> ImmutableMap.of();
}
