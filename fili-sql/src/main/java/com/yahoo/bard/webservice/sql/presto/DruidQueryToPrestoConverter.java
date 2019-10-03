/*
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
 */
package com.yahoo.bard.webservice.sql.presto;

import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.GROUP_BY;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;
import com.yahoo.bard.webservice.sql.DruidQueryToSqlConverter;
import com.yahoo.bard.webservice.sql.aggregation.DruidSqlAggregationConverter;
import com.yahoo.bard.webservice.sql.aggregation.SqlAggregation;
import com.yahoo.bard.webservice.sql.evaluator.FilterEvaluator;
import com.yahoo.bard.webservice.sql.evaluator.HavingEvaluator;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;
import com.yahoo.bard.webservice.sql.helper.SqlTimeConverter;
import com.yahoo.bard.webservice.table.SqlPhysicalTable;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Default implementation of converting a {@link DruidQuery} into a sql query.
 */
public class DruidQueryToPrestoConverter extends DruidQueryToSqlConverter {
    private static final Logger LOG = LoggerFactory.getLogger(DruidQueryToPrestoConverter.class);
    private final SqlTimeConverter sqlTimeConverter;
    private final BiFunction<Aggregation, ApiToFieldMapper, Optional<SqlAggregation>> druidSqlAggregationConverter;
    private final HavingEvaluator havingEvaluator;
    private final FilterEvaluator filterEvaluator;
    public static final int NO_OFFSET = -1;
    public static final int NO_LIMIT = -1;

    /**
     * Constructs the default converter.
     *
     * TODO could make an interface with {@link #isValidQuery(DruidQuery)} and
     * {@link #buildSqlQuery(DruidAggregationQuery, ApiToFieldMapper)}. Maybe make it a generic
     * more generic like "{@code DruidQueryConverter<T>}"
     *
     * @param calcitePrestoHelper  The calcite presto helper for this database.
     */
    public DruidQueryToPrestoConverter(CalciteHelper calcitePrestoHelper) {
        super(calcitePrestoHelper);
        this.sqlTimeConverter = buildSqlTimeConverter();
        this.druidSqlAggregationConverter = buildDruidSqlTypeConverter();
        this.havingEvaluator = new HavingEvaluator();
        this.filterEvaluator = new FilterEvaluator();
    }

    /**
     * Builds a time converter to designating how to translate between druid and sql
     * time information.
     *
     * @return a new time converter.
     */
    protected SqlTimeConverter buildSqlTimeConverter() {
        return new SqlTimeConverter();
    }

    /**
     * Builds a converter between druid and sql aggregations.
     *
     * @return a new druid to sql aggregation converter.
     */
    protected BiFunction<Aggregation, ApiToFieldMapper, Optional<SqlAggregation>> buildDruidSqlTypeConverter() {
        return new DruidSqlAggregationConverter();
    }

    /**
     * Converts the druid query to a {@link RelNode}.
     *
     * @param druidQuery  The query to convert to sql.
     * @param apiToFieldMapper  The mapping between api and physical names for the query.
     * @param sqlTable  The sql table being queried against.
     *
     * @return the sql equivalent of the query.
     */
    private RelNode convertDruidQueryToRelNode(
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            SqlPhysicalTable sqlTable
    ) {
        RelBuilder builder = calciteHelper.getNewRelBuilder(sqlTable.getSchemaName());

        builder = builder.scan(sqlTable.getName());

        return builder
                .filter(
                        getAllWhereFilters(builder, druidQuery, apiToFieldMapper, sqlTable.getTimestampColumn())
                )
                .aggregate(
                        builder.groupKey(getAllGroupByColumns(
                                builder,
                                druidQuery,
                                apiToFieldMapper,
                                sqlTable.getTimestampColumn()
                        )),
                        getAllQueryAggregations(builder, druidQuery, apiToFieldMapper)
                )
                .project(
                        (Iterable) ImmutableList.builder()
                                .addAll(builder.fields())
                                .addAll(getPostAggregations(builder, druidQuery, apiToFieldMapper))
                                .build()
                )
                .filter(
                        getHavingFilter(builder, druidQuery, apiToFieldMapper)
                )
                .sortLimit(
                        NO_OFFSET,
                        getLimit(druidQuery),
                        getSort(builder, druidQuery, apiToFieldMapper, sqlTable.getTimestampColumn())
                )
                .build();
    }


    /**
     * Finds the sorting for a druid query.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find the sorting from.
     * @param apiToFieldMapper  The mapping from api to physical names.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return a collection of rexnodes to apply sorts in calcite.
     */
    @Override
    protected List<RexNode> getSort(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            String timestampColumn
    ) {
        // druid does NULLS FIRST
        List<RexNode> sorts = new ArrayList<>();
        int timePartFunctions = sqlTimeConverter.timeGrainToDatePartFunctions(druidQuery.getGranularity()).size();
        int groupBys = druidQuery.getDimensions().size() + timePartFunctions;

        List<RexNode> limitSpecSorts = new ArrayList<>();
        Set<String> limitSpecColumns = new HashSet<>();
        if (druidQuery.getQueryType().equals(GROUP_BY)) {
            GroupByQuery groupByQuery = (GroupByQuery) druidQuery;
            LimitSpec limitSpec = groupByQuery.getLimitSpec();
            if (limitSpec != null) {
                limitSpec.getColumns()
                        .stream()
                        .map(orderByColumn -> {
                            String orderByField = apiToFieldMapper.unApply(orderByColumn.getDimension());
                            limitSpecColumns.add(orderByField);
                            RexNode sort = builder.literal(orderByField); //presto fix
                            if (orderByColumn.getDirection().equals(SortDirection.DESC)) {
                                sort = builder.desc(sort);
                            }
                            return sort;
                        })
                        .forEach(limitSpecSorts::add);
            }
        }
        // add time group by
        if (timePartFunctions == 0) {
            sorts.add(builder.field(timestampColumn));
        }
        sorts.addAll(builder.fields().subList(druidQuery.getDimensions().size(), groupBys));

        // add limit spec group by
        sorts.addAll(limitSpecSorts);

        // add remaining group by
        List<RexNode> unorderedDimensions = druidQuery.getDimensions().stream()
                .map(Dimension::getApiName)
                .map(apiToFieldMapper)
                .filter(columnName -> !limitSpecColumns.contains(columnName))
                .map(builder::field)
                .collect(Collectors.toList());
        sorts.addAll(unorderedDimensions);

        return sorts.stream()
                .map(sort -> builder.call(SqlStdOperatorTable.NULLS_FIRST, sort))
                .collect(Collectors.toList());
    }

    /**
     * Gets all the dimensions from a druid query as fields for calcite.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find the having filter from.
     * @param apiToFieldMapper  The mapping from api to physical name.
     *
     * @return the list of dimensions as {@link RexNode} for Calcite's builder.
     */
    private List<RexNode> getDimensionFields(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper
    ) {
        return druidQuery.getDimensions().stream()
                .map(Dimension::getApiName)
                .map(apiToFieldMapper)
                .map(builder::field)
                .collect(Collectors.toList());
    }


    /**
     * Find all druid aggregations and convert them to {@link org.apache.calcite.tools.RelBuilder.AggCall}.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The druid query to get the aggregations of.
     * @param apiToFieldMapper  The mapping from api to physical name.
     *
     * @return the list of aggregations.
     */
    @Override
    protected List<RelBuilder.AggCall> getAllQueryAggregations(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper
    ) {
        return druidQuery.getAggregations()
                .stream()
                .map(aggregation -> druidSqlAggregationConverter.apply(aggregation, apiToFieldMapper))
                .map(optionalSqlAggregation -> optionalSqlAggregation.orElseThrow(() -> {
                    String msg = "Couldn't build sql aggregation with " + optionalSqlAggregation;
                    LOG.debug(msg);
                    return new RuntimeException(msg);
                }))
                .map(sqlAggregation -> builder.aggregateCall(
                        sqlAggregation.getSqlAggFunction(),
                        false,
                        null,
                        CaseFormat.LOWER_UNDERSCORE.to(
                                CaseFormat.LOWER_CAMEL,
                                sqlAggregation.getSqlAggregationAsName()
                        ),
                        builder.field(sqlAggregation.getSqlAggregationFieldName())
                ))
                .collect(Collectors.toList());
    }

    private List<RexNode> getPostAggregations(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper
    ) {
        DruidPostAggregationToPresto druidPostAggregationToSql = new DruidPostAggregationToPresto();
        List<RexNode> fields = new ArrayList<>();
        druidQuery.getPostAggregations().stream()
                .map(postAggregation -> druidPostAggregationToSql
                        .evaluatePostAggregation(postAggregation, builder, apiToFieldMapper))
                .forEach(fields::add);
        return fields;
    }

    /**
     * Converts a RelBuilder into a sql string.
     *
     * @param sqlWriter  The writer to be used when translating the {@link org.apache.calcite.rel.RelNode} to sql.
     * @param relToSql  The converter from {@link org.apache.calcite.rel.RelNode} to
     * {@link org.apache.calcite.sql.SqlNode}.
     * @param query  The RelNode representing the query.
     *
     * @return the sql string built by the RelBuilder.
     */
    protected String writeSql(SqlPrettyWriter sqlWriter, RelToSqlConverter relToSql, RelNode query) {
        sqlWriter.reset();
        SqlSelect select = relToSql.visitChild(0, query).asSelect();
        return sqlWriter.format(select);
    }

    public SqlTimeConverter getTimeConverter() {
        return sqlTimeConverter;
    }
}
