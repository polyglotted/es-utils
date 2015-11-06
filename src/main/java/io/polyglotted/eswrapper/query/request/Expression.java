package io.polyglotted.eswrapper.query.request;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.polyglotted.eswrapper.query.ExpressionType;
import lombok.RequiredArgsConstructor;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

@RequiredArgsConstructor
public final class Expression {
    public static final Expression NilExpression = new Expression("_nil_", "_nil_", ImmutableMap.of(), ImmutableList.of());
    public static final String ValueKey = "_val";
    public final String operation;
    public final String label;
    public final ImmutableMap<String, Object> args;
    public final ImmutableList<Expression> children;

    public static Expression withMap(String operation, String label, java.util.Map<String, ?> args) {
        return new Expression(checkNotNull(operation), checkNotNull(label), ImmutableMap.copyOf(args), ImmutableList.of());
    }

    public static Expression withValue(ExpressionType expressionType, String label, Object valueArg) {
        return new Expression(expressionType.name(), checkNotNull(label), ImmutableMap.of(ValueKey, valueArg), ImmutableList.of());
    }

    public static <E extends Comparable<E>> Expression withArray(ExpressionType expressionType, String label, List<E> valueArg) {
        return new Expression(expressionType.name(), checkNotNull(label), ImmutableMap.of(ValueKey, valueArg), ImmutableList.of());
    }

    public static Expression withOnlyChildren(ExpressionType expressionType, String label, Iterable<Expression> list) {
        return new Expression(expressionType.name(), checkNotNull(label), ImmutableMap.of(), ImmutableList.copyOf(list));
    }

    public static Expression withLabel(ExpressionType expressionType, String label) {
        return new Expression(expressionType.name(), checkNotNull(label), ImmutableMap.of(), ImmutableList.of());
    }

    public <T> T valueArg() {
        return argFor(ValueKey, null);
    }

    public List<Object> arrayArg() {
        return argFor(ValueKey, ImmutableList.of());
    }

    public String stringArg() {
        return stringArg(ValueKey);
    }

    public String stringArg(String key) {
        return argFor(key, null);
    }

    public boolean boolArg(String key) {
        return argFor(key, Boolean.FALSE);
    }

    public int intArg(String key, int defValue) {
        return argFor(key, defValue);
    }

    public <T> T argFor(String key) {
        return checkNotNull(argFor(key, null));
    }

    @SuppressWarnings("unchecked")
    private <T> T argFor(String key, T defValue) {
        return args.containsKey(key) ? (T) args.get(key) : defValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Expression that = (Expression) o;
        return operation.equals(that.operation) && label.equals(that.label)
           && args.equals(that.args) && children.equals(that.children);
    }

    @Override
    public int hashCode() {
        return 19 * operation.hashCode() + 23 * label.hashCode() + 29 * args.hashCode() + 31 * children.hashCode();
    }

    @Override
    public String toString() {
        return (isNullOrEmpty(label) ? "" : label + " ") + operation + (args.isEmpty() ? "" : " " + args)
           + (children.isEmpty() ? "" : " " + children);
    }
}
