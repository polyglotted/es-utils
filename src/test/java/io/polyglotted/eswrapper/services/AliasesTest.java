package io.polyglotted.eswrapper.services;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.polyglotted.eswrapper.AbstractElasticTest;
import io.polyglotted.eswrapper.indexing.IndexSetting;
import io.polyglotted.eswrapper.indexing.TypeMapping;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static io.polyglotted.eswrapper.indexing.TypeMapping.typeBuilder;
import static io.polyglotted.pgmodel.search.index.Alias.aliasBuilder;
import static io.polyglotted.pgmodel.search.index.FieldMapping.simpleField;
import static io.polyglotted.pgmodel.search.index.FieldType.STRING;
import static io.polyglotted.pgmodel.search.query.Expressions.allIndex;
import static io.polyglotted.pgmodel.search.query.Expressions.liveIndex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AliasesTest extends AbstractElasticTest {

    private static final String INDEX_1 = "index_1";
    private static final String INDEX_2 = "index_2";
    private static final String INDEX_3 = "index_3";
    private static final String STAR_ALL = "*.all";
    private static final String STAR_LIVE = "*.live";

    @Override
    protected void performSetup() {
        admin.dropIndex(INDEX_1, INDEX_2, INDEX_3);
    }

    @Test
    public void basicStarAliasesTest() {
        createIndexWithStar(INDEX_1, true);
        Multimap<String, String> aliasData = admin.getAliasData(STAR_ALL);
        assertDefaultType(aliasData, STAR_ALL, of(INDEX_2, INDEX_3), INDEX_1);
        assertThat(aliasData.get(STAR_ALL), is(of("index_1:DefType", "index_1:*")));

        createIndexWithStar(INDEX_2, false);
        assertDefaultType(admin.getAliasData(STAR_ALL), STAR_ALL, of(INDEX_3), INDEX_1, INDEX_2);

        createIndexWithStar(INDEX_3, true);
        assertDefaultType(admin.getAliasData(STAR_ALL), STAR_ALL, of(), INDEX_1, INDEX_2, INDEX_3);

        admin.dropIndex(INDEX_2);
        assertDefaultType(admin.getAliasData(STAR_ALL), STAR_ALL, of(INDEX_2), INDEX_1, INDEX_3);
    }

    @Test
    public void getAliasIndicesTest() {
        createIndexWithStar(INDEX_1, false);
        createIndexWithStar(INDEX_3, false);
        createIndexWithStar(INDEX_2, false);
        assertThat(admin.getIndicesFor(STAR_ALL), is(ImmutableSet.of(INDEX_1, INDEX_2, INDEX_3)));
        assertThat(admin.getIndicesFor(STAR_LIVE), is(ImmutableSet.of(INDEX_1, INDEX_2, INDEX_3)));
    }

    private void assertDefaultType(Multimap<String, String> aliasData, String alias, List<String> nots, String... indices) {
        Collection<String> strings = aliasData.get(alias);
        for (String index : indices) assertThat(strings.contains(index + ":*"), is(true));
        for (String index : nots) assertThat(strings.contains(index + ":*"), is(false));
    }

    private void createIndexWithStar(String index, boolean addDefType) {
        admin.createIndex(IndexSetting.with(3, 1), index);
        mappings(index, addDefType).forEach(admin::createType);
        admin.updateAliases(aliasBuilder().alias(STAR_ALL).index(index).filter(allIndex()).build());
        admin.updateAliases(aliasBuilder().alias(STAR_LIVE).filter(liveIndex()).index(index).build());
    }

    private static List<TypeMapping> mappings(String index, boolean addDef) {
        ImmutableList.Builder<TypeMapping> builder = ImmutableList.builder();
        builder.add(typeBuilder().index(index).type("$lock").enableAll(false).enableSource(false).build());
        if (addDef) {
            builder.add(typeBuilder().index(index).type("DefType").fieldMapping(simpleField("a", STRING)).build());
            builder.add(typeBuilder().index(index).type("DefType$appr").fieldMapping(simpleField("a", STRING)).build());
        }
        return builder.build();
    }
}
