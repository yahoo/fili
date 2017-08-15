// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.sql.evaluator.PostAggregationEvaluator;
import com.yahoo.bard.webservice.sql.helper.SqlTimeConverter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Process the results from a DruidQuery to a sql backend.
 */
public class SqlResultSetProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SqlResultSetProcessor.class);
    private final DruidAggregationQuery<?> druidQuery;
    private final ApiToFieldMapper apiToFieldMapper;
    private BiMap<Integer, String> columnToColumnName;
    private List<String[]> sqlResults;
    private final ObjectMapper objectMapper;
    private final int groupByCount;
    private final SqlTimeConverter sqlTimeConverter;

    /**
     * Builds something to process a set of sql results and return them as the
     * same format as a GroupBy query to Druid.
     *
     * @param druidQuery  The original query that was converted to a sql query.
     * @param apiToFieldMapper  The mapping from api to physical name.
     * @param objectMapper  The mapper for all JSON processing.
     * @param sqlTimeConverter  The time converter used with making the query.
     */
    public SqlResultSetProcessor(
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            ObjectMapper objectMapper,
            SqlTimeConverter sqlTimeConverter
    ) {
        this.druidQuery = druidQuery;
        this.apiToFieldMapper = apiToFieldMapper;
        this.objectMapper = objectMapper;
        this.sqlTimeConverter = sqlTimeConverter;

        this.sqlResults = new ArrayList<>();
        this.columnToColumnName = HashBiMap.create();

        this.groupByCount = druidQuery.getDimensions().size();
    }

    /**
     * Processes the results from the sql {@link ResultSet} and writes them out as
     * the json format returned for a {@link com.yahoo.bard.webservice.druid.model.query.GroupByQuery}.
     *
     * @return the equivalent json.
     */
    public JsonNode buildDruidResponse() {
        Map<String, Function<String, Number>> resultTypeMapper = getAggregationTypeMapper(druidQuery);

        try (TokenBuffer jsonWriter = new TokenBuffer(objectMapper, true)) {

            jsonWriter.writeStartArray();
            for (String[] row : sqlResults) {
                jsonWriter.writeStartObject();
                
                DateTime timestamp;
                if (AllGranularity.INSTANCE.equals(druidQuery.getGranularity())) {
                    timestamp = druidQuery.getIntervals().get(0).getStart();
                } else {
                    timestamp = sqlTimeConverter.getIntervalStart(
                            groupByCount,
                            row,
                            druidQuery
                    );
                }
                // all druid results are returned in UTC timestamps
                jsonWriter.writeStringField("timestamp", timestamp.toDateTime(DateTimeZone.UTC).toString());
                jsonWriter.writeObjectFieldStart("event");

                processRow(resultTypeMapper, jsonWriter, row);

                jsonWriter.writeEndObject();
                jsonWriter.writeEndObject();
            }
            jsonWriter.writeEndArray();

            return jsonWriter.asParser().readValueAsTree();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write json.", e);
        }

    }

    /**
     * Processes a single row of results from the result set.
     *
     * @param resultTypeMapper  The mapping from an aggregation to a function which corrects it's type.
     * @param jsonWriter  The generator for writing the json results.
     * @param row  The result row.
     *
     * @throws IOException if failed while writing json.
     */
    protected void processRow(
            Map<String, Function<String, Number>> resultTypeMapper,
            JsonGenerator jsonWriter,
            String[] row
    ) throws IOException {
        int lastTimeIndex = sqlTimeConverter.timeGrainToDatePartFunctions(druidQuery.getGranularity()).size();
        int columnCount = columnToColumnName.size();

        for (int i = 0; i < columnCount; i++) {
            if (groupByCount <= i && i < groupByCount + lastTimeIndex) {
                continue;
            }
            String columnName = columnToColumnName.get(i);
            if (resultTypeMapper.containsKey(columnName)) {
                Number result = resultTypeMapper
                        .get(columnName)
                        .apply(row[i]);

                writeNumberField(jsonWriter, columnName, result);
            } else {
                jsonWriter.writeStringField(columnName, row[i]);
            }
        }

        PostAggregationEvaluator postAggregationEvaluator = new PostAggregationEvaluator();
        for (PostAggregation postAggregation : druidQuery.getPostAggregations()) {
            Number postAggResult = postAggregationEvaluator.calculate(
                    postAggregation,
                    (String columnName) -> row[columnToColumnName.inverse().get(columnName)]
            );
            writeNumberField(jsonWriter, postAggregation.getName(), postAggResult);
        }
    }

    /**
     * Reads the result set and converts it into a result that druid
     * would produce.
     *
     * @param resultSet  The result set of the druid query.
     *
     * @throws SQLException if results can't be read.
     */
    public void process(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int resultSetColumnCount = resultSetMetaData.getColumnCount();

        if (resultSetColumnCount != columnToColumnName.size() && columnToColumnName.size() != 0) {
            String msg = "Attempting to add ResultSet with " + resultSetColumnCount + " columns, but it should have "
                    + columnToColumnName.size() + " columns";
            LOG.warn(msg);
            throw new RuntimeException(msg);
        }

        if (columnToColumnName.size() == 0) {
            for (int i = 1; i <= resultSetColumnCount; i++) {
                String columnName = apiToFieldMapper.unApply(resultSetMetaData.getColumnName(i));
                columnToColumnName.put(i - 1, columnName);
            }
        }

        while (resultSet.next()) {
            String[] row = new String[resultSetColumnCount];
            for (int i = 1; i <= resultSetColumnCount; i++) {
                row[i - 1] = resultSet.getString(i);
            }
            sqlResults.add(row);
        }
    }

    /**
     * Writes a {@link Number} as either a {@link Double} or {@link Long} in json.
     *
     * @param jsonWriter  The writer used to build json.
     * @param name  The name of the field to write with json.
     * @param number  The Number value of the field to write with json.
     *
     * @throws IOException if results can't be written.
     */
    protected static void writeNumberField(JsonGenerator jsonWriter, String name, Number number) throws IOException {
        if (number instanceof Double) {
            jsonWriter.writeNumberField(name, (Double) number);
        } else if (number instanceof Long) {
            jsonWriter.writeNumberField(name, (Long) number);
        }
    }

    /**
     * Creates a map from each aggregation name, i.e. ("longSum", "doubleSum"),
     * to a function which will parse to the correct type, i.e. (long, double).
     * If no type is found it will do nothing.
     *
     * @param druidQuery  The query to make a mapper for.
     *
     * @return the map from aggregation name to {@link Double#parseDouble} {@link Long#parseLong}.
     */
    protected static Map<String, Function<String, Number>> getAggregationTypeMapper(
            DruidAggregationQuery<?> druidQuery
    ) {
        //todo maybe "true"/"false" -> boolean
        return druidQuery.getAggregations()
                .stream()
                .collect(
                        Collectors.toMap(
                                Aggregation::getName,
                                aggregation -> {
                                    String aggType = aggregation.getType().toLowerCase(Locale.ENGLISH);
                                    if (aggType.contains("long")) {
                                        return Long::parseLong;
                                    } else if (aggType.contains("double")) {
                                        return Double::parseDouble;
                                    }
                                    return null;
                                }
                        )
                );
    }
}
