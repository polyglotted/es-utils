package io.polyglotted.eswrapper.indexing;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Objects;

import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.deleteRecord;
import static io.polyglotted.eswrapper.indexing.IndexRecord.updateRecord;

@RequiredArgsConstructor
@ToString(includeFieldNames = false, doNotUseGetters = true)
public final class SleeveDoc<T> {
    public final IndexKey key;
    public final T source;

    public static <T> List<SleeveDoc<T>> createSleeves(List<T> objects, Function<T, SleeveDoc<T>> newSleeveFunction) {
        return Lists.transform(objects, newSleeveFunction);
    }

    public static <T> SleeveDoc<T> newSleeve(T object, Function<T, IndexKey> keyFunction) {
        return new SleeveDoc<>(keyFunction.apply(object), object);
    }

    public static <T> List<SleeveDoc<T>> deleteSleeves(List<IndexKey> keys) {
        return Lists.transform(keys, SleeveDoc::deleteSleeve);
    }

    public static <T> SleeveDoc<T> deleteSleeve(IndexKey key) {
        return new SleeveDoc<>(key.delete(), null);
    }

    public boolean isNew() {
        return key.version <= 0;
    }

    public boolean isDelete() {
        return key.delete;
    }

    public IndexRecord toRecord(Function<T, String> function) {
        return isNew() ? createRecord(key.type, key.id).source(function.apply(source)).build()
           : (isDelete() ? deleteRecord(key) : updateRecord(key).source(function.apply(source)).build());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SleeveDoc other = (SleeveDoc) o;
        return key.equals(other.key) && source.equals(other.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, source);
    }
}
