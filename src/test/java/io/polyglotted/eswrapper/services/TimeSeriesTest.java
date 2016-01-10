package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexRecord;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.Indexable;
import io.polyglotted.pgmodel.search.SimpleDoc;
import io.polyglotted.pgmodel.search.query.*;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.testng.annotations.Test;

import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;

import static io.polyglotted.eswrapper.indexing.Bundling.bundlingBuilder;
import static io.polyglotted.eswrapper.indexing.IgnoreErrors.strict;
import static io.polyglotted.eswrapper.indexing.IndexRecord.createRecord;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.query.ResultBuilder.SimpleDocBuilder;
import static io.polyglotted.pgmodel.search.IndexKey.keyWithParent;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedField;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedStringField;
import static io.polyglotted.pgmodel.search.index.FieldType.DATE;
import static io.polyglotted.pgmodel.search.query.Aggregates.avgBuilder;
import static io.polyglotted.pgmodel.search.query.Aggregates.childrenAggBuilder;
import static io.polyglotted.pgmodel.search.query.Aggregates.countBuilder;
import static io.polyglotted.pgmodel.search.query.Aggregates.dateHistogramBuilder;
import static io.polyglotted.pgmodel.search.query.Aggregates.filterAggBuilder;
import static io.polyglotted.pgmodel.search.query.Aggregates.termBuilder;
import static io.polyglotted.pgmodel.search.query.Expressions.between;
import static io.polyglotted.pgmodel.search.query.Expressions.equalsTo;
import static io.polyglotted.pgmodel.search.query.Expressions.hasParent;
import static io.polyglotted.pgmodel.search.query.Expressions.or;
import static io.polyglotted.pgmodel.search.query.Flattened.flattened;
import static io.polyglotted.pgmodel.search.query.StandardQuery.queryBuilder;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TimeSeriesTest extends AbstractElasticTest {
    private static final String TS_INDEX = "time_series";
    private static final String SRA_TYPE = "SeriesA";
    private static final String PTA_TYPE = "SeriesA-Point";
    private static final String SRB_TYPE = "SeriesB";
    private static final String PTB_TYPE = "SeriesB-Point";
    private static final String ID_FIELD = "id";
    private static final String CAT_FIELD = "cat";
    private static final String DATE_FIELD = "date";

    @Override
    protected void performSetup() {
        admin.dropIndex(TS_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), TS_INDEX);
        admin.createType(typeBuilder().index(TS_INDEX).type(SRA_TYPE)
           .fieldMapping(notAnalyzedStringField(ID_FIELD)).fieldMapping(notAnalyzedStringField(CAT_FIELD)).build());
        admin.createType(typeBuilder().index(TS_INDEX).type(SRB_TYPE)
           .fieldMapping(notAnalyzedStringField(ID_FIELD)).fieldMapping(notAnalyzedStringField(CAT_FIELD)).build());

        admin.createType(typeBuilder().index(TS_INDEX).type(PTA_TYPE).parent(SRA_TYPE)
           .fieldMapping(notAnalyzedField(DATE_FIELD, DATE)).build());
        admin.createType(typeBuilder().index(TS_INDEX).type(PTB_TYPE).parent(SRB_TYPE)
           .fieldMapping(notAnalyzedField(DATE_FIELD, DATE)).build());
    }

    @Test
    public void timeSeriesTest() {
        indexSeriesAndPoints();

        Expression expr = or(hasParent(SRA_TYPE, equalsTo(CAT_FIELD, "series")),
           hasParent(SRB_TYPE, equalsTo(CAT_FIELD, "series")), equalsTo(CAT_FIELD, "series"));

        Aggregates.Builder histo = termBuilder("id", "_parent").add(dateHistogramBuilder("dates", "date", "2w")
           .add(avgBuilder("avg", "value")));
        Expression aggr = filterAggBuilder("applies-to", between("date",
           ZonedDateTime.of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli(),
           ZonedDateTime.of(2015, 6, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli()))
           .add(histo).build();

        QueryResponse response = query.search(queryBuilder().index(TS_INDEX).size(10000)
           .expression(expr).aggregate(aggr).build(), null, SimpleDocBuilder);
        List<SimpleDoc> simpleDocs = response.resultsAs(SimpleDoc.class);
        assertThat(simpleDocs.size(), is(202));

        int flattenCount = 0;
        Iterator<Flattened> iterator = Flattened.flatten(response.aggregations.get(0)).iterator();
        while (iterator.hasNext()) {
            iterator.next();
            flattenCount++;
        }
        assertThat(flattenCount, is(22));
    }

    @Test
    public void childrenAggregation() {
        indexSeriesAndPoints();

        Aggregates.Builder aggs = termBuilder("Series", CAT_FIELD).add(childrenAggBuilder("child", PTB_TYPE)
           .add(countBuilder("cnt", "value")));
        Aggregation aggregation = query.aggregate(aggs.build(), TS_INDEX);

        Iterator<Flattened> iterator = Flattened.flatten(aggregation).iterator();
        assertEquals(iterator.next(), flattened("series", "child", 100L));
        assertEquals(iterator.next(), flattened("series2", "child", 100L));
        assertFalse(iterator.hasNext());
    }

    public void indexSeriesAndPoints() {
        long ts1 = 1425494500000L, ts2 = 1425494600000L;

        SeriesA a = new SeriesA("a/a1", "series");
        SeriesB b1 = new SeriesB("b/b1", "series", true);
        SeriesB b2 = new SeriesB("b/b2", "series2", false);
        Indexable indexable = indexableBuilder().timestamp(ts1).user("unit-tester").records(asList(
           createRecord(TS_INDEX, SRA_TYPE, a.id).source(GSON.toJson(a)).build(),
           createRecord(TS_INDEX, SRB_TYPE, b1.id).source(GSON.toJson(b1)).build(),
           createRecord(TS_INDEX, SRB_TYPE, b2.id).source(GSON.toJson(b2)).build()
        )).build();
        indexer.twoPhaseCommit(indexable);

        indexer.bulkIndex(bundlingBuilder().timestamp(ts2).records(makePoints(TS_INDEX, PTA_TYPE, a.id, 100)).build());
        indexer.bulkIndex(bundlingBuilder().timestamp(ts2).records(makePoints(TS_INDEX, PTB_TYPE, b1.id, 100)).build());
        indexer.bulkIndex(bundlingBuilder().timestamp(ts2).records(makePoints(TS_INDEX, PTB_TYPE, b2.id, 100)).build());
    }

    private static List<IndexRecord> makePoints(String index, String type, String parent, int numPoints) {
        ZonedDateTime dateTime = Instant.ofEpochMilli(1446336000000L).atZone(UTC);
        SecureRandom random = new SecureRandom();

        ImmutableList.Builder<IndexRecord.Builder> records = ImmutableList.builder();
        for (int i = 0; i < numPoints; i++) {
            Point point = new Point(dateTime.toInstant().toEpochMilli(), random.nextDouble());
            records.add(createRecord(keyWithParent(index, type, parent)).source(GSON.toJson(point)));

            dateTime = dateTime.minusWeeks(1);
        }
        return Lists.transform(records.build(), IndexRecord.Builder::build);
    }

    @EqualsAndHashCode(doNotUseGetters = true)
    @RequiredArgsConstructor
    private static class SeriesA {
        public final String id;
        public final String cat;
    }

    @EqualsAndHashCode(doNotUseGetters = true)
    @RequiredArgsConstructor
    private static class SeriesB {
        public final String id;
        public final String cat;
        public final boolean foo;
    }

    @ToString(includeFieldNames = false)
    @EqualsAndHashCode(doNotUseGetters = true)
    @RequiredArgsConstructor
    private static class Point {
        public final long date;
        public final double value;
    }
}
