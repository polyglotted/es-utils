package io.polyglotted.eswrapper.indexing;

import org.testng.annotations.Test;

import static io.polyglotted.eswrapper.indexing.IgnoreErrors.from;
import static io.polyglotted.eswrapper.indexing.IgnoreErrors.lenient;
import static io.polyglotted.eswrapper.indexing.IgnoreErrors.strict;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class IgnoreErrorsTest {

    @Test
    public void testStrict() {
        assertFalse(strict().ignoreFailure("DocumentAlreadyExistsException thrown"));
        assertFalse(strict().ignoreFailure("VersionConflictEngineException thrown"));
        assertFalse(strict().ignoreFailure("AnyOtherException thrown"));
    }

    @Test
    public void testLenient() {
        assertTrue(lenient().ignoreFailure("DocumentAlreadyExistsException thrown"));
        assertTrue(lenient().ignoreFailure("VersionConflictEngineException thrown"));
        assertFalse(lenient().ignoreFailure("AnyOtherException thrown"));
    }

    @Test
    public void testIgnoreDocAlreadyExists() {
        assertTrue(from(true, false).ignoreFailure("DocumentAlreadyExistsException thrown"));
        assertFalse(from(true, false).ignoreFailure("VersionConflictEngineException thrown"));
        assertFalse(from(true, false).ignoreFailure("AnyOtherException thrown"));
    }

    @Test
    public void testIgnoreVersionConflict() {
        assertFalse(from(false, true).ignoreFailure("DocumentAlreadyExistsException thrown"));
        assertTrue(from(false, true).ignoreFailure("VersionConflictEngineException thrown"));
        assertFalse(from(false, true).ignoreFailure("AnyOtherException thrown"));
    }
}