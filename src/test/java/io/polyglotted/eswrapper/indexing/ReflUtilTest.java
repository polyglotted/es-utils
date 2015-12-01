package io.polyglotted.eswrapper.indexing;

import org.testng.annotations.Test;

public class ReflUtilTest extends ReflUtil {

    @Test(expectedExceptions = IllegalStateException.class)
    public void fieldValueFail() {
        fieldValue(new NotFoundA(), "bar", "baz");
    }

    @SuppressWarnings("unchecked")
    static final class NotFoundA {
        private String foo;
    }
}