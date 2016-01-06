package io.polyglotted.eswrapper.indexing;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.pgmodel.search.SimpleDoc;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static io.polyglotted.pgmodel.search.IndexKey.keyWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class IndexableHelperTest extends IndexableHelper {

    @DataProvider
    public static Object[][] roleDocInputs() {
        return new Object[][]{
           {of(), of(), true},
           {of(), of("A", "B"), true},
           {of(new SimpleDoc(keyWith("a", "b", "c"), ImmutableMap.of())), of("A", "B"), true},
           {of(new SimpleDoc(keyWith("a", "b", "c"), ImmutableMap.of("&approvalRoles", of("A", "D")))), of("A", "B"), true},
           {of(new SimpleDoc(keyWith("a", "b", "c"), ImmutableMap.of("&approvalRoles", of("C", "B")))), of("A", "B"), true},
           {of(new SimpleDoc(keyWith("a", "b", "c"), ImmutableMap.of("&approvalRoles", of("C", "D")))), of("A", "B"), false},
        };
    }

    @Test(dataProvider = "roleDocInputs")
    public void validateApprovalRolesReturns(List<SimpleDoc> docs, List<String> roles, boolean expected) {
        assertThat(validateApprovalRoles(docs, roles), is(expected));
    }
}