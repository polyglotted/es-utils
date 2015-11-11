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
public class Nested {
    public static final String NESTED_TYPE = "Nested";
    public final String target;
    public final Child child;

    public static Nested nestedFrom(String target, String effect) {
        return new Nested(target, new Child(effect));
    }

    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class Child {
        public final String effect;
    }

    public static BulkRequest nestedRequest(String index) {
        List<Nested> nesteds = nesteds();
        BulkRequest bulkRequest = new BulkRequest().refresh(true);
        for (Nested nest : nesteds) {
            bulkRequest.add(new IndexRequest(index, NESTED_TYPE).create(true).source(GSON.toJson(nest)));
        }
        return bulkRequest;
    }

    public static List<Nested> nesteds() {
        List<Nested> nesteds = new ArrayList<>(3);
        nesteds.add(nestedFrom("mike", "funny boy chap"));
        nesteds.add(nestedFrom("bob", "grumpy proud.don fella"));
        nesteds.add(nestedFrom("vinny", "proud.don chap"));
        return nesteds;
    }
}
