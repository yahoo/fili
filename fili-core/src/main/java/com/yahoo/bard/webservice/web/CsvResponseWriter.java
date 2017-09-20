// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.metric.MetricColumn;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.stream.Stream;

/**
 * Serializer for CSV format.
 */
public class CsvResponseWriter implements ResponseWriter {

    private final ObjectMappersSuite objectMappers;
    private static final Logger LOG = LoggerFactory.getLogger(CsvResponseWriter.class);
    /**
     * Constructor.
     *
     * @param objectMappers  ObjectMappersSuite object needed for CsvMapper
     */
    public CsvResponseWriter(ObjectMappersSuite objectMappers) {
        this.objectMappers = objectMappers;
    }

    @Override
    public void write(
            ApiRequest request,
            ResponseData responseData,
            OutputStream outputStream
    ) throws IOException {
        // Just write the header first
        CsvSchema schema = buildCsvHeaders(responseData);
        CsvMapper csvMapper = objectMappers.getCsvMapper();
        csvMapper.writer().with(schema.withSkipFirstDataRow(true))
                .writeValue(outputStream, Collections.emptyMap());

        ObjectWriter writer = csvMapper.writer().with(schema.withoutHeader());

        try {
            responseData.getResultSet().stream()
                    .map(responseData::buildResultRow)
                    .forEachOrdered(
                            row -> {
                                try {
                                    writer.writeValue(outputStream, row);
                                } catch (IOException ioe) {
                                    String msg = String.format("Unable to write CSV data row: %s", row);
                                    LOG.error(msg, ioe);
                                    throw new RuntimeException(msg, ioe);
                                }
                            }
                    );
        } catch (RuntimeException re) {
            throw new IOException(re);
        }
    }

    /**
     * Builds the CSV header.
     *
     * @param responseData  Data object containing all the result information
     *
     * @return The CSV schema with the header
     */
    public CsvSchema buildCsvHeaders(ResponseData responseData) {
        CsvSchema.Builder builder = CsvSchema.builder();
        Stream.concat(
                Stream.of("dateTime"),
                Stream.concat(
                        responseData.getRequestedApiDimensionFields()
                                .entrySet().stream().flatMap(responseData::generateDimensionColumnHeaders),
                        responseData.getApiMetricColumns().stream().map(MetricColumn::getName)
                )
        ).forEachOrdered(builder::addColumn);
        return builder.setUseHeader(true).build();
    }
}
