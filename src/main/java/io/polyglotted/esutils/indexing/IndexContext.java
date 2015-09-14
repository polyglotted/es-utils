package io.polyglotted.esutils.indexing;

import org.elasticsearch.action.bulk.BulkRequest;

public interface IndexContext {



    BulkRequest bulkRequest();

    Iterable<String> indices();

    //how about validation?

    default boolean refreshIndex() {
        return true;
    }
}
