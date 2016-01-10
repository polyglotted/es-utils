package io.polyglotted.eswrapper.services;

import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse.Failure;
import org.testng.annotations.Test;

public class ModelIndexUtilTest extends ModelIndexUtil {

    @Test(expectedExceptions = IllegalStateException.class,
    expectedExceptionsMessageRegExp = "error multi-get item a/b/c: foo failed")
    public void testFailure() throws Exception {
        checkMultiGet(new MultiGetItemResponse(null, new Failure("a", "b", "c", "foo failed")));
    }
}