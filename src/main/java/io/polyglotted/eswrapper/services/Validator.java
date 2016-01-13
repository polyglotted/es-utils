package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.pgmodel.search.IndexKey;
import io.polyglotted.pgmodel.search.SimpleDoc;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface Validator {

    Map<String, String> validate(Collection<IndexKey> keys, List<SimpleDoc> docs);

    Validator EMPTY_VALIDATOR = (keys, docs) -> ImmutableMap.of();
}
