package io.polyglotted.eswrapper.indexing;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IgnoreErrors {
    private final byte id;

    private static final int STRICT = 0;
    private static final int IGNORE_DOC_ALREADY_EXISTS = 1;
    private static final int IGNORE_VERSION_CONFLICT = 2;

    private static final IgnoreErrors VALUES[];

    static {
        byte max = 1 << 2;
        VALUES = new IgnoreErrors[max];
        for (byte id = 0; id < max; id++) {
            VALUES[id] = new IgnoreErrors(id);
        }
    }

    public boolean ignoreFailure(String message) {
        return message == null
           || ((id & IGNORE_DOC_ALREADY_EXISTS) != 0 && message.startsWith("DocumentAlreadyExistsException"))
           || ((id & IGNORE_VERSION_CONFLICT) != 0 && message.startsWith("VersionConflictEngineException"));
    }

    public static IgnoreErrors strict() {
        return from(false, false);
    }

    public static IgnoreErrors lenient() {
        return from(true, true);
    }

    public static IgnoreErrors from(boolean ignoreDocumentAlreadyExists, boolean ignoreVersionConflict) {
        byte id = 0;
        if (ignoreDocumentAlreadyExists) {
            id |= IGNORE_DOC_ALREADY_EXISTS;
        }
        if (ignoreVersionConflict) {
            id |= IGNORE_VERSION_CONFLICT;
        }
        return VALUES[id];
    }
}
