package io.polyglotted.eswrapper.services;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;
import java.util.List;

import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class SortableText {
    public static final String SORTABLE_TYPE = "SortableText";
    public final String name;
    public final String desc;

    public static BulkRequest textsRequest(String index) {
        List<SortableText> sortables = sortables();
        BulkRequest bulkRequest = new BulkRequest().refresh(true);
        for (SortableText text : sortables) {
            bulkRequest.add(new IndexRequest(index, SORTABLE_TYPE).create(true).source(GSON.toJson(text)));
        }
        return bulkRequest;
    }

    public static List<SortableText> sortables() {
        List<SortableText> sortables = new ArrayList<>(3);
        sortables.add(new SortableText("john", "awesome cook"));
        sortables.add(new SortableText("bill", "brilliant amazing boy"));
        sortables.add(new SortableText("pete", "lovely chap"));
        return sortables;
    }
}
