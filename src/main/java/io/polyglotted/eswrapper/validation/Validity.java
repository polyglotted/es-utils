package io.polyglotted.eswrapper.validation;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

@ToString(doNotUseGetters = true, includeFieldNames = false)
@RequiredArgsConstructor
public final class Validity {
    public final List<Memo> memos;

    public boolean isValid() {
        return memos.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) && memos.equals(((Validity) o).memos));
    }

    @Override
    public int hashCode() {
        return 37 * memos.hashCode();
    }

    public static Builder validityBuilder() {
        return new Builder();
    }

    public static Validity validity(String key, String message) {
        return validityBuilder().memo(key, message).build();
    }

    public static Validity valid() {
        return new Validity(ImmutableList.of());
    }

    @ToString(doNotUseGetters = true, includeFieldNames = false)
    @RequiredArgsConstructor
    public static class Memo {
        public final String key;
        public final String message;

        @Override
        public boolean equals(Object o) {
            return this == o || (!(o == null || getClass() != o.getClass())
               && key.equals(((Memo) o).key) && message.equals(((Memo) o).message));
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, message);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final List<Memo> memos = new LinkedList<>();

        public Builder memo(String key, String message) {
            memos.add(new Memo(checkNotNull(key), checkNotNull(message)));
            return this;
        }

        public Validity build() {
            return new Validity(ImmutableList.copyOf(memos));
        }
    }
}
