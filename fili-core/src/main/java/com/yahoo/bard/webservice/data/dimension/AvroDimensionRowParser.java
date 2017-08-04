// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Parses an AVRO file into Dimension Rows.
 */
public class AvroDimensionRowParser {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDimensionRowParser.class);

    private final DimensionFieldNameMapper dimensionFieldNameMapper;

    /**
     * Constructs an AvroDimensionRowParser object based on the DimensionFieldNameMapper object.
     *
     * @param dimensionFieldNameMapper Object that defines the dimension field name transformations
     */
    public AvroDimensionRowParser(DimensionFieldNameMapper dimensionFieldNameMapper) {
        this.dimensionFieldNameMapper = memoize(dimensionFieldNameMapper);
    }

    /**
     * Validates the schema of the given AVRO file with the Dimension schema configured by the user.
     *
     * <p>
     * The sample schema is expected to be in the following format
     * <pre><code>
     * {
     *    "type" : "record",
     *    "name" : "TUPLE_0",
     *    "fields" : [
     *    {
     *    "name" : "FOO_ID",
     *    "type" : [ "null","int" ]
     *    },
     *    {
     *    "name" : "FOO_DESC",
     *    "type" : [ "null", "string" ]
     *    }
     *    ]
     * }
     * </code></pre>
     *
     * @param dimension The dimension object used to configure the dimension
     * @param avroSchema The AVRO Schema
     *
     * @return true if the schema is valid, false otherwise
     *
     * @throws IllegalArgumentException thrown if JSON object `fields` is not present
     */
    private boolean doesSchemaContainAllDimensionFields(Dimension dimension, Schema avroSchema)
        throws IllegalArgumentException {

        // Extract field names
        Set<String> avroFields = avroSchema.getFields().stream()
                        .map(Field::name)
                        .collect(Collectors.toSet());

        // True only if all of the mapped dimension fields are present in the Avro schema
        return dimension.getDimensionFields().stream()
                .map(dimensionField -> dimensionFieldNameMapper.convert(dimension, dimensionField))
                .allMatch(avroFields::contains);
    }


    /**
     * Transform an avro generic record into a set map of fields and values.
     *
     * @param genericRecord  The avro record being read
     * @param dimension  The dimension for the row being loaded
     *
     * @return  A map of fields and values for a dimension row
     */
    private Map<String, String> recordToMap(GenericRecord genericRecord, Dimension dimension) {
        return dimension.getDimensionFields()
                .stream()
                .collect(
                        Collectors.toMap(
                                DimensionField::getName,
                                dimensionField -> resolveRecordValue(
                                        genericRecord,
                                        dimensionFieldNameMapper.convert(dimension, dimensionField)
                                )
                        ));
    }

    /**
     * Returns a stream which parses avro records into dimension rows.
     *
     * @param dataFileReader  An open file reader for avro records
     * @param dimension  The dimension object used to configure the dimension
     *
     * @return A stream over the open file which produces dimension rows
     *
     * @throws IllegalArgumentException thrown if JSON object `fields` is not present
     */
    private Stream<DimensionRow> streamDimensionRows(
            DataFileReader<GenericRecord> dataFileReader,
            Dimension dimension
    ) throws IllegalArgumentException {
        // Validate Schema
        if (!doesSchemaContainAllDimensionFields(dimension, dataFileReader.getSchema())) {
            String msg = "The AVRO schema file does not contain all the configured dimension fields";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        // Generates a set of dimension Rows after retrieving the appropriate fields
        return StreamSupport.stream(dataFileReader.spliterator(), false)
                .map(record -> recordToMap(record, dimension))
                .map(dimension::parseDimensionRow);

    }

    /**
     * Parses the avro file and sends dimension rows to a consumer.
     *
     * @param dimension  The dimension object used to configure the dimension
     * @param avroFilePath  The path of the AVRO data file (.avro)
     * @param consumer  A consumer to process records from the avro file
     *
     * @throws IllegalArgumentException thrown if JSON object `fields` is not present
     */
    public void parseAvroFileDimensionRows(Dimension dimension, String avroFilePath, Consumer<DimensionRow> consumer)
            throws IllegalArgumentException {
        GenericDatumReader datumReader = new GenericDatumReader();

        // Creates an AVRO DataFileReader object that reads the AVRO data file one record at a time
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(avroFilePath), datumReader)) {

            streamDimensionRows(dataFileReader, dimension).forEach(consumer);

        } catch (IOException e) {
            String msg = String.format("Unable to process the file, at the location %s", avroFilePath);
            LOG.error(msg, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    /**
     * Parses the avro file and returns the dimension rows.
     *
     * @param dimension The dimension object used to configure the dimension
     * @param avroFilePath The path of the AVRO data file (.avro)
     *
     * @return A set of dimension rows
     *
     * @throws IllegalArgumentException thrown if JSON object `fields` is not present
     */
    public Set<DimensionRow> parseAvroFileDimensionRows(Dimension dimension, String avroFilePath)
        throws IllegalArgumentException {
        GenericDatumReader  datumReader = new GenericDatumReader();

        // Creates an AVRO DataFileReader object that reads the AVRO data file one record at a time
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(avroFilePath), datumReader)) {

            return streamDimensionRows(dataFileReader, dimension).collect(Collectors.toSet());

        } catch (IOException e) {
            String msg = String.format("Unable to process the file, at the location %s", avroFilePath);
            LOG.error(msg, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    /**
     * Returns a memoized converter function for the dimension field name mapping.
     *
     * @param dimensionFieldNameMapper Object that defines the dimension field name transformations
     * @return Memoized function that converts the dimension field name based on the user mapping
     */
    private DimensionFieldNameMapper memoize(DimensionFieldNameMapper dimensionFieldNameMapper) {
        Map<Pair<Dimension, DimensionField>, String> cache = new HashMap<>();
        return (dimension, dimensionField) -> cache.computeIfAbsent(
                new Pair<>(dimension, dimensionField),
                key -> dimensionFieldNameMapper.convert(key.getKey(), key.getValue())
        );
    }

    /**
     * Retrieve the given field in the generic record, replace null value record with string "null".
     *
     * @param genericRecord  One of the record in the given avro file
     * @param dimensionFieldName  Name of the dimension field used to retrieve the value from the record
     *
     * @return string representing the record value
     */
    private String resolveRecordValue(GenericRecord genericRecord, String dimensionFieldName) {
        Object result = genericRecord.get(dimensionFieldName);
        return Objects.isNull(result) ? "" : result.toString();
    }
}
