// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.response.DruidResponse;
import com.yahoo.bard.webservice.druid.response.DruidResultRow;
import com.yahoo.bard.webservice.druid.response.GroupByResultRow;
import com.yahoo.bard.webservice.druid.response.TimeseriesResultRow;
import com.yahoo.bard.webservice.druid.response.TopNResultRow;

import com.google.common.collect.BiMap;

import org.joda.time.DateTime;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Process the results from a DruidQuery to a sql backend.
 */
public class SqlResultSetProcessor {
    private final DruidAggregationQuery<?> druidQuery;
    private final BiMap<Integer, String> columnNames;
    private final List<String[]> sqlResults;

    public SqlResultSetProcessor(
            DruidAggregationQuery<?> druidQuery,
            BiMap<Integer, String> columnNames,
            List<String[]> sqlResults
    ) {
        this.druidQuery = druidQuery;
        this.columnNames = columnNames;
        this.sqlResults = sqlResults;
    }

    public DruidResponse process() {
        Map<String, Function<String, Object>> resultTypeMapper = getAggregationTypeMapper(druidQuery);
        int columnCount = columnNames.size();
        int groupByCount = druidQuery.getDimensions().size();

        DruidResponse druidResponse = new DruidResponse();
        sqlResults.stream()
                .map(row -> {
                    DateTime timestamp = TimeConverter.parseDateTime(groupByCount, row, druidQuery.getGranularity());

                    DruidResultRow rowResult = getDruidResultRow(timestamp);

                    int lastTimeIndex = TimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
                    for (int i = 0; i < columnCount; i++) {
                        if (groupByCount <= i && i < groupByCount + lastTimeIndex) {
                            continue;
                        }
                        Object result = resultTypeMapper
                                .getOrDefault(columnNames.get(i), String::toString)
                                .apply(row[i]);
                        rowResult.add(columnNames.get(i), result);
                    }

                    druidQuery.getPostAggregations()
                            .forEach(postAggregation -> {
                                Double postAggResult = PostAggregationEvaluator.evaluate(
                                        postAggregation,
                                        (s) -> row[columnNames.inverse().get(s)]
                                );
                                rowResult.add(postAggregation.getName(), postAggResult);
                            });

                    return rowResult;
                })
                .forEach(druidResponse::add);

        return druidResponse;
    }

    private DruidResultRow getDruidResultRow(DateTime timestamp) {
        DruidResultRow rowResult;
        if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
            rowResult = new GroupByResultRow(timestamp, GroupByResultRow.Version.V1);
        } else if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            rowResult = new TopNResultRow(timestamp);
        } else {
            rowResult = new TimeseriesResultRow(timestamp);
        }
        return rowResult;
    }

    /**
     * Creates a map from each aggregation name, i.e. ("longSum", "doubleSum"),
     * to a function which will parse to the correct type, i.e. (long, double).
     * If no type is found it will do nothing.
     *
     * @param druidQuery  The query to make a mapper for.
     *
     * @return the map from aggregation name to {@link Double::parseDouble} {@link Long::parseLong}.
     */
    private static Map<String, Function<String, Object>> getAggregationTypeMapper(
            DruidAggregationQuery<?> druidQuery
    ) {
        //todo maybe "true"/"false" -> boolean
        return druidQuery.getAggregations()
                .stream()
                .collect(Collectors.toMap(Aggregation::getName, aggregation -> {
                    String aggType = aggregation.getType().toLowerCase(Locale.ENGLISH);
                    if (aggType.contains("long")) {
                        return Long::parseLong;
                    } else if (aggType.contains("double")) {
                        return Double::parseDouble;
                    }
                    return String::toString;
                }));
    }
}
