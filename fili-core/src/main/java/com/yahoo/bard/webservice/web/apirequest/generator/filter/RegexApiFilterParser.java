// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID;

import com.yahoo.bard.webservice.util.FilterTokenizer;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadFilterException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

/**
 * {@link ApiFilterParser} that uses the regex stored in {@link RegexApiFilterParser#API_FILTER_PATTERN} to parse
 * individual filters in {@link FilterDefinition}s, and {@link RegexApiFilterParser#COMMA_AFTER_BRACKET_PATTERN} to
 * split filters.
 */
public class RegexApiFilterParser implements ApiFilterParser {

    private static final Logger LOG = LoggerFactory.getLogger(RegexApiFilterParser.class);

    /**
     * Pattern used to split filters. Splits on a comma preceded by a bracket.
     */
    public static final String COMMA_AFTER_BRACKET_PATTERN = "(?<=]),";

    /**
     * Regex used to parse single filters.
     */
    public static final Pattern API_FILTER_PATTERN = Pattern.compile("([^\\|]+)\\|([^-]+)-([^\\[]+)\\[([^\\]]+)\\]?");

    @Override
    public FilterDefinition parseSingleApiFilterQuery(@NotNull String singleFilter) throws BadFilterException {
        if (singleFilter == null || singleFilter.isEmpty()) {
            throw new IllegalArgumentException("Single filter parsing requires the input to be non-null and non-empty");
        }

        Matcher matcher = API_FILTER_PATTERN.matcher(singleFilter);

        if (!matcher.matches()) {
            LOG.debug(FILTER_INVALID.logFormat(singleFilter));
            throw new BadFilterException(FILTER_INVALID.format(singleFilter));
        }

        // replaceAll takes care of any leading ['s or trailing ]'s which might mess up this.values
        try {
            return new FilterDefinition(
                    matcher.group(1),
                    matcher.group(2),
                    matcher.group(3),
                    new ArrayList<>(
                            FilterTokenizer.split(
                                    matcher.group(4)
                                            .replaceAll("\\[", "")
                                            .replaceAll("\\]", "")
                                            .trim())
                    )
            );
        } catch (IllegalArgumentException e) {
            throw new BadFilterException(FILTER_ERROR.logFormat(singleFilter, e.getMessage()), e);
        }
    }

    @Override
    public List<FilterDefinition> parseApiFilterQuery(String apiFilterQuery) throws BadFilterException {
        if (apiFilterQuery == null || apiFilterQuery.isEmpty()) {
            return Collections.emptyList();
        }

        List<FilterDefinition> result = new ArrayList<>();
        for (String singleFilter : Arrays.asList(apiFilterQuery.split(COMMA_AFTER_BRACKET_PATTERN))) {
            result.add(parseSingleApiFilterQuery(singleFilter));
        }
        return result;
    }
}
