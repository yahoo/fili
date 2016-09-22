// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.util.Pagination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.core.UriInfo;

/**
 * Formats data of a response as CSV.
 *
 * @param <T> the type of the raw data
 */
public class CsvResponse<T> extends AbstractResponse<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvResponse.class);

    private final List<String> columnNames;

    /**
     * Constructor.
     *
     * @param entries  The data entries to generate the response for.
     * @param pages  The paginated set of results containing the pages being linked to.
     * @param uriInfo  UriInfo to generate the URL for the page links.
     * @param columnNames  The CSV header. If null this class will try to extract the header given that the entries
     * represent maps with strings as keys.
     * @param objectMappers  Suite of Object Mappers to use when serializing the response.
     */
    public CsvResponse(
            Stream<T> entries,
            Pagination<?> pages,
            UriInfo uriInfo,
            List<String> columnNames,
            ObjectMappersSuite objectMappers
    ) {
        super(entries, pages, uriInfo, objectMappers);
        this.columnNames = columnNames;
    }

    /**
     * Writes CSV response.
     *
     * @param os  The output stream to write document bytes to
     *
     * @throws IOException If an error occurs while writing this stream
     */
    @Override
    public void write(OutputStream os) throws IOException {
        AtomicReference<CsvSchema> schema = new AtomicReference<>();
        AtomicBoolean isFirstRow = new AtomicBoolean(true);

        ObjectMapper csvMapper = objectMappers.getCsvMapper();

        try {
            entries.peek(row -> schema.compareAndSet(null, setOrGuessHeader(row, columnNames)))
                    .forEachOrdered(
                            row -> {
                                try {
                                    boolean addHeader = isFirstRow.getAndSet(false);
                                    csvMapper.writer().with(schema.get().withUseHeader(addHeader)).writeValue(os, row);
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
     * Get the header (schema) of the response.
     *
     * @param entry  Entry to examine if we need to guess at the header (ie. we have no column names)
     * @param columnNames  Column names to use for the header if we have them
     * @param <T>  Type of the entry we're examining
     *
     * @return the schema (header)
     */
    private static <T> CsvSchema setOrGuessHeader(T entry, List<String> columnNames) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = ((Map<String, ?>) entry);
            return buildCsvHeaders(
                    columnNames != null ?
                            columnNames :
                            map.entrySet().stream()
                                    .filter(e -> e.getValue() != null)
                                    .map(Map.Entry::getKey)
                                    .collect(Collectors.toList())
            );
        } catch (ClassCastException cce) {
            String msg = "Unable to extract CSV column names from data. Headers need to be specified explicitly.";
            LOG.error(msg, cce);
            throw new RuntimeException(msg, cce);
        }
    }

    /**
     * Builds the CSV header.
     *
     * @param columns  Columns to use for building the header
     *
     * @return CSV schema with the header
     */
    private static CsvSchema buildCsvHeaders(List<String> columns) {
        CsvSchema.Builder builder = CsvSchema.builder();
        columns.stream().forEachOrdered(builder::addColumn);
        return builder.setUseHeader(true).build();
    }
}
