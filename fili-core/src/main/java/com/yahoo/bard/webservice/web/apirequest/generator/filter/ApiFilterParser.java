// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter;

import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadFilterException;

import java.util.List;

import javax.validation.constraints.NotNull;

/**
 * Contract for parsing String based api filter queries {@link FilterDefinition} instances. FilterDefinitions are data
 * objects that represent filter queries parsed into their component pieces. The filter parser does <b>not</b> convert
 * the parsed FilterDefinition objects into complete {@link ApiFilter} instances.
 */
public interface ApiFilterParser {

    /**
     * Convenience method for parsing a SINGLE string form api filter into a {@link FilterDefinition}. The filter query
     * must NOT be null nor empty. {@link ApiFilterParser#parseApiFilterQuery(String)} is intended for parsing input
     * directly from the user and CAN handle null or empty filter queries.
     *
     * @param singleFilter  The non-empty string form filter to parse.
     * @return the FilterDefinition representing the parsed filter.
     * @throws BadFilterException if the filter malformed.
     * @throws IllegalArgumentException if the filter is null or empty
     */
    FilterDefinition parseSingleApiFilterQuery(@NotNull String singleFilter) throws BadFilterException;

    /**
     * Parses a complete api filter clauses into a List of {@link FilterDefinition}s. This method is intended to handle
     * direct user input. Null and empty filter queries result in an empty list being returned. If any piece of the
     * filter query is malformed a {@link BadFilterException} will be thrown. In this case, even correctly formed
     * filters will be lost.
     *
     * @param apiFilterQuery  The filter query to parse
     * @return the list of FilterDefinitions representing all of the filters parsed from the input filter query
     * @throws BadFilterException If any piece of the filter query is malformed.
     */
    List<FilterDefinition> parseApiFilterQuery(String apiFilterQuery) throws BadFilterException;
}
