// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.presto;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;
import com.yahoo.bard.webservice.sql.SqlResultSetProcessor;
import com.yahoo.bard.webservice.sql.helper.SqlTimeConverter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Process the results from a DruidQuery to a sql backend.
 */
public class PrestoResultSetProcessor extends SqlResultSetProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PrestoResultSetProcessor.class);

    /**
     * Builds something to process a set of sql results and return them as the
     * same format as a GroupBy query to Druid.
     *
     * @param druidQuery  The original query that was converted to a sql query.
     * @param apiToFieldMapper  The mapping from api to physical name.
     * @param objectMapper  The mapper for all JSON processing.
     * @param sqlTimeConverter  The time converter used with making the query.
     */
    public PrestoResultSetProcessor(
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            ObjectMapper objectMapper,
            SqlTimeConverter sqlTimeConverter
    ) {
        super(druidQuery, apiToFieldMapper, objectMapper, sqlTimeConverter);
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
    @Override
    protected void processRow(
            Map<String, Function<String, Number>> resultTypeMapper,
            JsonGenerator jsonWriter,
            String[] row
    ) throws IOException {
        int lastTimeIndex = sqlTimeConverter.timeGrainToDatePartFunctions(druidQuery.getGranularity()).size();
        int columnCount = columnToColumnName.size();

        for (int i = 0; i < columnCount; i++) {
            if (isTimeColumn(lastTimeIndex, i)) {
                continue;
            }
            String columnName = columnToColumnName.get(i);
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
}
