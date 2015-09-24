package io.polyglotted.eswrapper.services;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;
import java.util.List;

import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static java.util.Arrays.asList;

@RequiredArgsConstructor
public class NamePath {
    public static final String NAMEPATH_TYPE = "NamePath";

    public final String name;
    public final List<String> path;

    @SafeVarargs
    public NamePath(String name, String... paths) {
        this(name, asList(paths));
    }

    public static BulkRequest pathsRequest(String index) {
        List<NamePath> paths = new ArrayList<>(3);
        paths.add(new NamePath("john", "/users/main/john", "/employees/perm/john"));
        paths.add(new NamePath("bill", "/users/aux/bill", "/employees/perm/bill"));
        paths.add(new NamePath("pete", "/users/aux/pete"));
        BulkRequest bulkRequest = new BulkRequest().refresh(true);
        for (NamePath path : paths) {
            bulkRequest.add(new IndexRequest(index, NAMEPATH_TYPE).create(true).source(GSON.toJson(path)));
        }
        return bulkRequest;
    }
}
