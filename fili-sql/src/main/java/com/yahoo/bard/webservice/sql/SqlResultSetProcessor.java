// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
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
    protected final DruidAggregationQuery<?> druidQuery;
    protected final ApiToFieldMapper apiToFieldMapper;
    private BiMap<Integer, String> columnToColumnName;


    private List<String[]> sqlResults;
    private final ObjectMapper objectMapper;
    private final int groupByDimensionsCount;
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

        this.groupByDimensionsCount = druidQuery.getDimensions().size();
    }

    /**
     * Processes the results from the sql {@link ResultSet} and writes them out as
     * the json format returned for a {@link com.yahoo.bard.webservice.druid.model.query.GroupByQuery}.
     *
     * @return the equivalent json.
     */
    public JsonNode buildDruidResponse() {
        Map<String, Function<String, Number>> resultTypeMapper = getAggregationTypeMapper(druidQuery);

        try (TokenBuffer jsonWriter = new TokenBuffer(getObjectMapper(), true)) {

            jsonWriter.writeStartArray();
            for (String[] row : getSqlResults()) {
                jsonWriter.writeStartObject();

                DateTime timestamp;
                if (AllGranularity.INSTANCE.equals(druidQuery.getGranularity())) {
                    timestamp = druidQuery.getIntervals().get(0).getStart();
                } else {
                    timestamp = getSqlTimeConverter().getIntervalStart(
                            groupByDimensionsCount,
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
        int lastTimeIndex = getSqlTimeConverter().timeGrainToDatePartFunctions(druidQuery.getGranularity()).size();
        int columnCount = getColumnToColumnName().size();

        for (int i = 0; i < columnCount; i++) {
            if (isTimeColumn(lastTimeIndex, i)) {
                continue;
            }
            String columnName = getColumnToColumnName().get(i);
            if (resultTypeMapper.containsKey(columnName)) {
                if (row[i] == null) {
                    jsonWriter.writeNullField(columnName);
                } else {
                    Number result = resultTypeMapper
                            .get(columnName)
                            .apply(row[i]);
                    writeNumberField(jsonWriter, columnName, result);
                }
            } else {
                jsonWriter.writeStringField(columnName, row[i]);
            }
        }
    }

    /**
     * Checks whether the current position in a row is a raw column or an exploded date time column.
     *
     * @param lastTimeIndex  The index of the last column that is part of the exploded date time.
     * @param currentIndex  The current index in the row.
     *
     * @return true if the current index is of an exploded date time column.
     */
    protected boolean isTimeColumn(int lastTimeIndex, int currentIndex) {
        return currentIndex >= getGroupByDimensionsCount() && currentIndex
                < getGroupByDimensionsCount() + lastTimeIndex;
    }

    /**
     * Reads the result set and converts it into a result that druid
     * would produce.
     *
     * @param sqlResultSet  The result set of the druid query.
     *
     * @throws SQLException if results can't be read.
     */
    public void process(ResultSet sqlResultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = sqlResultSet.getMetaData();
        int resultSetColumnCount = resultSetMetaData.getColumnCount();

        if (resultSetColumnCount != getColumnToColumnName().size() && getColumnToColumnName().size() != 0) {
            String msg = "Attempting to add ResultSet with " + resultSetColumnCount + " columns, but it should have "
                    + getColumnToColumnName().size() + " columns";
            LOG.warn(msg);
            throw new RuntimeException(msg);
        }

        if (getColumnToColumnName().size() == 0) {
            for (int i = 1; i <= resultSetColumnCount; i++) {
                String columnName = apiToFieldMapper.unApply(resultSetMetaData.getColumnName(i));
                getColumnToColumnName().put(i - 1, columnName);
            }
        }

        while (sqlResultSet.next()) {
            String[] row = new String[resultSetColumnCount];
            for (int i = 1; i <= resultSetColumnCount; i++) {
                row[i - 1] = sqlResultSet.getString(i);
            }
            getSqlResults().add(row);
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

    private static Function<String, Number> getNumParseFunctionByAggType(Aggregation agg) {
        String aggType = agg.getType().toLowerCase(Locale.ENGLISH);
        if (aggType.contains("long")) {
            return Long::parseLong;
        } else if (aggType.contains("double")) {
            return Double::parseDouble;
        } else if (aggType.contains("count")) {
            return Long::parseLong;
        } else if (aggType.contains("filtered") && agg instanceof FilteredAggregation) {
            return getNumParseFunctionByAggType(((FilteredAggregation) agg).getAggregation());
        }
        return null;
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
        // todo see https://github.com/yahoo/fili/issues/510
        //todo maybe "true"/"false" -> boolean
        return druidQuery.getAggregations()
                .stream()
                .collect(
                        Collectors.toMap(
                                Aggregation::getName,
                                aggregation -> getNumParseFunctionByAggType(aggregation)
                        )
                );
    }

    protected BiMap<Integer, String> getColumnToColumnName() {
        return columnToColumnName;
    }

    protected List<String[]> getSqlResults() {
        return sqlResults;
    }

    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    protected int getGroupByDimensionsCount() {
        return groupByDimensionsCount;
    }

    protected SqlTimeConverter getSqlTimeConverter() {
        return sqlTimeConverter;
    }
}
