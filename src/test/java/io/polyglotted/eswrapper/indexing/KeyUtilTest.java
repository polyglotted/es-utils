package io.polyglotted.eswrapper.indexing;

import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class KeyUtilTest extends KeyUtil {

    @Test
    public void testIndexKey() {
        System.out.println(System.currentTimeMillis());
        String orig = new IndexKey("abc", "", "id101", 1234).uniqueId();

        assertThat(orig, is("0fbce132-d525-5bea-8bba-d4d21fdbb7b1"));
        assertThat(orig, is(equalTo(new IndexKey("abc", "", "id101", 1234).uniqueId())));
        assertThat(orig, is(not(equalTo(new IndexKey("def", "", "id101", 1234).uniqueId()))));
        assertThat(orig, is(not(equalTo(new IndexKey("abc", "", "id102", 1234).uniqueId()))));
        assertThat(orig, is(not(equalTo(new IndexKey("abc", "", "id101", 1245).uniqueId()))));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testCreateMessageDigestFail() {
        createMessageDigest("abcd");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testToOutputStreamFail() {
        IndexKey.writeToStream(new IndexKey("a", "b", "c", 1), new OutputStream() {
            public void write(int b) throws IOException {
                throw new IOException();
            }
        });
    }
}