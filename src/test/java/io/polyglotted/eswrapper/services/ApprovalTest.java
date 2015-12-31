package io.polyglotted.eswrapper.services;

import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexRecord;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.Indexable;
import io.polyglotted.pgmodel.search.Sleeve;
import io.polyglotted.pgmodel.search.index.FieldType;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.indexing.IndexRecord.forApproval;
import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;
import static io.polyglotted.eswrapper.indexing.Indexable.indexableBuilder;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.eswrapper.services.IndexableTest.fetchRecords;
import static io.polyglotted.eswrapper.services.IndexableTest.newSleeveFunction;
import static io.polyglotted.eswrapper.services.Trade.FieldDate;
import static io.polyglotted.eswrapper.services.Trade.TRADE_TYPE;
import static io.polyglotted.eswrapper.services.Trade.sampleTrades;
import static io.polyglotted.pgmodel.search.Sleeve.createSleeves;
import static io.polyglotted.pgmodel.search.index.Alias.aliasBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.notAnalyzedField;
import static io.polyglotted.pgmodel.search.query.Expressions.archiveIndex;
import static io.polyglotted.pgmodel.search.query.Expressions.liveIndex;
import static io.polyglotted.pgmodel.search.query.Expressions.pendingApproval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ApprovalTest extends AbstractElasticTest {
    private static final String APPROVAL_INDEX = "approval_index";
    private static final String APPROVAL_LIVE = "approval_index.live";
    private static final String APPROVAL_HISTORY = "approval_index.history";
    private static final String APPROVAL_TYPE = TRADE_TYPE + "$approval";
    private static final long T1 = 1442784057000L;
    private static final long T2 = 1442784062000L;

    @Override
    protected void performSetup() {
        admin.dropIndex(APPROVAL_INDEX);
        admin.createIndex(IndexSetting.with(3, 0), APPROVAL_INDEX);

        admin.createType(typeBuilder().index(APPROVAL_INDEX).type(TRADE_TYPE)
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());
        admin.createType(typeBuilder().index(APPROVAL_INDEX).type(APPROVAL_TYPE)
           .fieldMapping(notAnalyzedField(FieldDate, FieldType.DATE)).build());

        admin.updateAliases(aliasBuilder().alias(APPROVAL_LIVE).index(APPROVAL_INDEX).filter(liveIndex()).build(),
           aliasBuilder().alias(APPROVAL_HISTORY).index(APPROVAL_INDEX).filter(archiveIndex()).build());
    }

    @Test
    public void createAndApprove() {
        List<Sleeve<Trade>> sleeves = createSleeves(sampleTrades(), newSleeveFunction(APPROVAL_INDEX, APPROVAL_TYPE));
        indexer.twoPhaseCommit(pendingIndexable(sleeves, T1));

        assertThat(fetchRecords(query, APPROVAL_LIVE).size(), is(0));
        assertThat(fetchRecords(query, APPROVAL_INDEX, pendingApproval()).size(), is(20));
    }

    private static Indexable approveIndexable(Iterable<Sleeve<Trade>> sleeveDocs, long ts) {
        return indexableBuilder().user("unit-tester").timestamp(ts).records(sleeveToRecords(sleeveDocs)).build();
    }

    private static Indexable pendingIndexable(Iterable<Sleeve<Trade>> sleeveDocs, long ts) {
        return indexableBuilder().user("unit-tester").timestamp(ts).records(sleeveToRecords(sleeveDocs)).build();
    }

    private static Iterable<IndexRecord> sleeveToRecords(Iterable<Sleeve<Trade>> sleeveDocs) {
        return transform(sleeveDocs, doc -> forApproval(doc, sl -> GSON.toJson(sl.source)));
    }
}
