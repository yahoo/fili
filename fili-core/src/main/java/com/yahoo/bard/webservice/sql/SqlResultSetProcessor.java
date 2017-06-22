// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.sql.evaluator.PostAggregationEvaluator;
import com.yahoo.bard.webservice.sql.helper.TimeConverter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.collect.BiMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
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
    private final BiMap<Integer, String> columnToColumnName;
    private final List<String[]> sqlResults;
    private final ObjectMapper objectMapper;
    private final int columnCount;
    private final int groupByCount;

    public SqlResultSetProcessor(
            DruidAggregationQuery<?> druidQuery,
            BiMap<Integer, String> columnToColumnName,
            List<String[]> sqlResults,
            ObjectMapper objectMapper
    ) {
        this.druidQuery = druidQuery;
        this.columnToColumnName = columnToColumnName;
        this.sqlResults = sqlResults;
        this.objectMapper = objectMapper;

        this.groupByCount = druidQuery.getDimensions().size();
        this.columnCount = columnToColumnName.size();
    }

    public JsonNode process() {
        Map<String, Function<String, Number>> resultTypeMapper = getAggregationTypeMapper(druidQuery);

        try (TokenBuffer jsonWriter = new TokenBuffer(objectMapper, true)) {

            jsonWriter.writeStartArray();
            for (String[] row : sqlResults) {
                processRow(resultTypeMapper, jsonWriter, row);
            }
            jsonWriter.writeEndArray();

            return jsonWriter.asParser().readValueAsTree();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write json.", e);
        }

    }

    private DateTime processRow(
            Map<String, Function<String, Number>> resultTypeMapper,
            JsonGenerator jsonWriter,
            String[] row
    ) throws IOException {
        DateTime timestamp = TimeConverter.parseDateTime(groupByCount, row, druidQuery.getGranularity());
        jsonWriter.writeStartObject();
        jsonWriter.writeStringField("timestamp", timestamp.toDateTime(DateTimeZone.UTC).toString());
        jsonWriter.writeObjectFieldStart("event");

        int lastTimeIndex = TimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
        for (int i = 0; i < columnCount; i++) {
            if (groupByCount <= i && i < groupByCount + lastTimeIndex) {
                continue;
            }
            String columnName = columnToColumnName.get(i);
            if (resultTypeMapper.containsKey(columnName)) {
                Number result = resultTypeMapper
                        .get(columnName)
                        .apply(row[i]);
                if (result instanceof Long) {
                    jsonWriter.writeNumberField(columnName, (long) result);
                } else {
                    jsonWriter.writeNumberField(columnName, (double) result);
                }
            } else {
                jsonWriter.writeStringField(columnName, row[i]);
            }

        }

        for (PostAggregation postAggregation : druidQuery.getPostAggregations()) {
            Double postAggResult = PostAggregationEvaluator.evaluate(
                    postAggregation,
                    (String columnName) -> row[columnToColumnName.inverse().get(columnName)]
            );
            jsonWriter.writeNumberField(postAggregation.getName(), postAggResult);
        }

        jsonWriter.writeEndObject();
        jsonWriter.writeEndObject();

        return timestamp;
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
    private static Map<String, Function<String, Number>> getAggregationTypeMapper(
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
                    return null;
                }));
    }
}
