package io.polyglotted.esutils.query.request;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import static com.google.common.base.Preconditions.checkNotNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SimpleSort {
    public final String field;
    public final String order;
    public final String mode;
    public final String unmappedType;
    public final Object missing;

    public static SimpleSort sortAsc(String field) {
        return sortBuilder().field(field).build();
    }

    public static SimpleSort sortDesc(String field) {
        return sortBuilder().field(field).order(Order.DESC).build();
    }

    public enum Order {
        ASC, DESC
    }

    @SuppressWarnings("unused")
    public enum Mode {
        NONE, MIN, MAX, SUM, AVG;

        public String toMode() {
            return NONE == this ? null : name().toLowerCase();
        }
    }

    public static Builder sortBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String field;
        private Order order = Order.ASC;
        private Mode mode = Mode.NONE;
        private String unmappedType;
        private Object missing;

        public SimpleSort build() {
            return new SimpleSort(checkNotNull(field, "field should not be null"), order.name(),
                    mode.toMode(), unmappedType, missing);
        }
    }
}
