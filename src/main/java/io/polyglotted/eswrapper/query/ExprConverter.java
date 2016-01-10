package io.polyglotted.eswrapper.query;

import io.polyglotted.pgmodel.search.query.Expression;
import org.elasticsearch.index.query.FilterBuilder;

import java.util.List;

import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.eswrapper.ElasticConstants.ALL_META;
import static io.polyglotted.eswrapper.query.QueryBuilder.toStrArray;
import static io.polyglotted.pgmodel.search.query.Expression.NilExpression;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.MatchQueryBuilder.Type.PHRASE_PREFIX;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

public enum ExprConverter {
    All {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return matchAllFilter();
        }
    },
    Ids {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            List<String> types = expr.argFor("types");
            return idsFilter(toStrArray(types)).ids(toStrArray(transform(expr.arrayArg(), Object::toString)));
        }
    },
    Eq {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return termFilter(expr.label, (Object) expr.valueArg());
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
            return prefixFilter(expr.label, expr.stringArg());
        }
    },
    Ne {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return notFilter(termFilter(expr.label, (Object) expr.valueArg()));
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
            String field = isNullOrEmpty(expr.label) ? ALL_META : expr.label;
            return queryFilter(matchQuery(field, expr.valueArg()).type(PHRASE_PREFIX));
        }
    },
    Regex {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return regexpFilter(expr.label, expr.stringArg());
        }
    },
    Exists {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return existsFilter(expr.label);
        }
    },
    Missing {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return missingFilter(expr.label).existence(true).nullValue(true);
        }
    },
    Type {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return typeFilter(expr.label);
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
    },
    HasParent {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return hasParentFilter(expr.label, aggregateFilters(expr.children)[0]);
        }
    },
    HasChild {
        @Override
        FilterBuilder buildFrom(Expression expr) {
            return hasChildFilter(expr.label, aggregateFilters(expr.children)[0]);
        }
    };

    abstract FilterBuilder buildFrom(Expression expr);

    public static FilterBuilder buildFilter(Expression expr) {
        return expr == null || NilExpression.equals(expr) ? null : valueOf(expr.operation).buildFrom(expr);
    }

    public static FilterBuilder[] aggregateFilters(Iterable<Expression> expressions) {
        return toArray(filter(transform(expressions, ExprConverter::buildFilter), notNull()), FilterBuilder.class);
    }
}
