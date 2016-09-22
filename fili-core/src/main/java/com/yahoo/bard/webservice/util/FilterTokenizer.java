// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class to tokenize Strings based on CSV properties.
 */
public class FilterTokenizer {

    private static final String PARSING_FAILURE = "Parsing values '%s' failed";
    private static final String PARSING_FAILURE_UNQOUTED_VALUES_FORMAT = "Set of values '%s' cannot be processed.  "
        + "Empty strings, commas, and leading or trailing whitespace must be quoted";

    // Sharing ObjectReaders/ObjectWriters is recommended as more efficient vs sharing ObjectMapper. It is thread-safe.
    private static ObjectReader defaultReader = initDefault();
    private static ObjectReader rawReader = initRaw();

    /**
     * Initialize an ObjectReader.
     *
     * @return an initialized ObjectReader
     */
    protected static ObjectReader initDefault() {
        return init(CsvSchema.emptySchema());
    }

    /**
     * Initialize an ObjectReader that does not quote things.
     *
     * @return an initialized ObjectReader
     */
    protected static ObjectReader initRaw() {
        return init(CsvSchema.emptySchema().withoutQuoteChar());
    }

    /**
     * Initialize the tokenizer.
     *
     * @param filterCsvSchema  Schema to initialize the tokenizer for
     *
     * @return an initialized ObjectReader
     */
    protected static ObjectReader init(CsvSchema filterCsvSchema) {
        return new CsvMapper()
                .disable(CsvParser.Feature.WRAP_AS_ARRAY)
                .readerFor(String[].class)
                .with(filterCsvSchema);
    }

    /**
     * Split a string into tokens based on CSV rules.
     * The CSV parsing defined here uses comma as a delimeter,
     * double quotes for escaping characters, including comma and double quote itself,
     * and does not use a special character for escaping (such as '\').
     * If double quotes need to be escaped inside the field, then the whole field needs
     * to be double quoted (see Examples below).
     * <p>
     * Examples:
     * Foo, Bar corresponds to ["Foo", "Bar"]
     * "Foo, Bar and Baz",Qux  corresponds to ["Foo, Bar and Baz", "Qux"]
     * Foo,"2'10""" corresponds to ["Foo", "2'10\""]
     * Foo,,Bar is invalid. Empty string was not quoted
     *
     * @param input the string to split
     *
     * @return list of tokens. The size of this list is fixed.
     * @throws IllegalArgumentException if the input has empty items
     */
    public static List<String> split(String input) throws IllegalArgumentException {
        try {
            // Since there's no way to check whether a field was an empty string surrounded
            // by double quotes or not after parsing it as a CSV string with the default
            // parser, we use two parsers: One with the default schema and one that does
            // not translate quotes
            String[] result = defaultReader.readValue(input);
            String[] raw = rawReader.readValue(input);

            // If the one that doesn't translate quotes includes empty strings (after trimming)
            // then it's unclear whether the use of empty field was intended or not by the user
            // and therefore an exception is thrown
            for (String item : raw) {
                if (item.trim().isEmpty()) {
                    String msg = String.format(PARSING_FAILURE_UNQOUTED_VALUES_FORMAT, input);
                    throw new IllegalArgumentException(msg);
                }
            }
            return Arrays.asList(result);
        } catch (IOException ioe) {
            String msg = String.format(PARSING_FAILURE, input);
            throw new IllegalArgumentException(msg, ioe);
        }
    }
}
