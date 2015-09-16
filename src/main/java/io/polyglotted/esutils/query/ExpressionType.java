package io.polyglotted.esutils.query;

import io.polyglotted.esutils.query.request.Expression;
import org.elasticsearch.index.query.FilterBuilder;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.MatchQueryBuilder.Type.PHRASE_PREFIX;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;

public enum ExpressionType {
    Eq {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return termFilter(expr.label, expr.stringArg());
        }
    },
    Gte {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return rangeFilter(expr.label).gte(expr.valueArg());
        }
    },
    Gt {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return rangeFilter(expr.label).gt(expr.valueArg());
        }
    },
    Lte {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return rangeFilter(expr.label).lte(expr.valueArg());
        }
    },
    Lt {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return rangeFilter(expr.label).lt(expr.valueArg());
        }
    },
    Prefix {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return queryFilter(prefixQuery(expr.label, expr.stringArg()));
        }
    },
    Ne {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return notFilter(termFilter(expr.label, expr.stringArg()));
        }
    },
    In {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return inFilter(expr.label, toArray(expr.arrayArg(), Object.class));
        }
    },
    Between {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return rangeFilter(expr.label).from(expr.argFor("from")).to(expr.argFor("to"))
               .includeLower(expr.boolArg("fromIncl")).includeUpper(expr.boolArg("toIncl"));
        }
    },
    Text {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            String field = isNullOrEmpty(expr.label) ? "_all" : expr.label;
            return queryFilter(matchQuery(field, expr.valueArg()).type(PHRASE_PREFIX));
        }
    },
    Regex {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return regexpFilter(expr.label, expr.stringArg());
        }
    },
    Missing {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return missingFilter(expr.label).existence(true).nullValue(true);
        }
    },
    Json {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return wrapperFilter(expr.stringArg());
        }
    },
    And {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return andFilter(aggregateFilters(expr.children));
        }
    },
    Or {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return orFilter(aggregateFilters(expr.children));
        }
    },
    Not {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return notFilter(aggregateFilters(expr.children)[0]);
        }
    },
    Nested {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return nestedFilter(expr.label, aggregateFilters(expr.children)[0]);
        }
    };

    abstract FilterBuilder buildFrom(Expression expr);

    public static FilterBuilder buildFilter(Expression expr) {
        return expr == null ? null : valueOf(expr.operation).buildFrom(expr);
    }

    public static FilterBuilder[] aggregateFilters(Iterable<Expression> expressions) {
        return toArray(transform(expressions, ExpressionType::buildFilter), FilterBuilder.class);
    }
}
