package io.polyglotted.eswrapper.indexing;

import com.google.common.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static java.util.Base64.getDecoder;

abstract class KeyUtil {

    private static final ThreadLocal<MessageDigest> SHA1 = new ThreadLocal<MessageDigest>() {
        @Override
        protected MessageDigest initialValue() {
            return createMessageDigest("SHA1");
        }
    };

    public static UUID generateUuid(byte[] nameBytes) {
        ByteBuffer buffer = ByteBuffer.wrap(sha1Digest(nameBytes)).order(ByteOrder.BIG_ENDIAN);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static byte[] sha1Digest(byte[] bytes) {
        MessageDigest sha1 = SHA1.get();
        sha1.reset();
        sha1.update(getDecoder().decode("E1vHqXA4RYe9TgfVsyyKtw=="));
        sha1.update(bytes);
        byte[] digest = sha1.digest();
        digest[0x06] = (byte) ((digest[0x06] & 0xF) | 0x05 << 4);
        digest[0x08] = (byte) ((digest[0x08] & 0x3F) | 0x80);
        return digest;
    }

    @VisibleForTesting
    static MessageDigest createMessageDigest(String algo) {
        try {
            return MessageDigest.getInstance(algo);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
