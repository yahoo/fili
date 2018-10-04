// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_FIELD_NOT_IN_DIMENSIONS;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.FilterTokenizer;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadFilterException;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.FilterOperation;
import com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * This utility class captures default implementations for binding and validating API models for filtering requests.
 */
public class FilterBinders {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBinders.class);

    protected static final String COMMA_AFTER_BRACKET_PATTERN = "(?<=]),";
    protected static Pattern API_FILTER_PATTERN = Pattern.compile("([^\\|]+)\\|([^-]+)-([^\\[]+)\\[([^\\]]+)\\]?");

    public static final FilterBinders INSTANCE = new FilterBinders();

    /**
     * Generates api filter objects on the based on the filter query value in the request parameters.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * (dimension name).(fieldname)-(operation):[?(value or comma separated values)]?
     * @param table  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     * @throws BadApiRequestException if the filter query string does not match required syntax, or the filter
     * contains a 'startsWith' or 'contains' operation while the BardFeatureFlag.DATA_STARTS_WITH_CONTAINS_ENABLED is
     * off.
     */
    public ApiFilters generateFilters(
            String filterQuery,
            LogicalTable table,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingFilters")) {
            LOG.trace("Dimension Dictionary: {}", dimensionDictionary);
            // Set of filter objects
            ApiFilters generated = new ApiFilters();

            // Filters are optional hence check if filters are requested.
            if (filterQuery == null || filterQuery.isEmpty()) {
                return generated;
            }

            // split on '],' to get list of filters
            List<String> apiFilters = Arrays.asList(filterQuery.split(COMMA_AFTER_BRACKET_PATTERN));
            for (String apiFilter : apiFilters) {
                ApiFilter newFilter;
                try {
                    newFilter = generateApiFilter(apiFilter, dimensionDictionary);

                    // If there is a logical table and the filter is not part of it, throw exception.
                    if (!table.getDimensions().contains(newFilter.getDimension())) {
                        String filterDimensionName = newFilter.getDimension().getApiName();
                        LOG.debug(FILTER_DIMENSION_NOT_IN_TABLE.logFormat(filterDimensionName, table));
                        String errorMessage = FILTER_DIMENSION_NOT_IN_TABLE.format(
                                filterDimensionName,
                                table.getName()
                        );
                        throw new BadApiRequestException(errorMessage, new BadFilterException(errorMessage));
                    }

                } catch (BadFilterException filterException) {
                    throw new BadApiRequestException(filterException.getMessage(), filterException);
                }

                if (!BardFeatureFlag.DATA_FILTER_SUBSTRING_OPERATIONS.isOn()) {
                    FilterOperation filterOperation = newFilter.getOperation();
                    if (filterOperation.equals(DefaultFilterOperation.startswith)
                            || filterOperation.equals(DefaultFilterOperation.contains)
                            ) {
                        throw new BadApiRequestException(
                                ErrorMessageFormat.FILTER_SUBSTRING_OPERATIONS_DISABLED.format()
                        );

                    }
                }
                Dimension dim = newFilter.getDimension();
                if (!generated.containsKey(dim)) {
                    generated.put(dim, new LinkedHashSet<>());
                }
                generated.get(dim).add(newFilter);
            }
            LOG.trace("Generated map of filters: {}", generated);

            return generated;
        }
    }

    /**
     * Parses the URL filter Query and generates the ApiFilter object.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * <code>(dimension name)|(field name)-(operation)[?(value or comma separated values)]?</code>
     * @param dimensionDictionary  cache containing all the valid dimension objects.
     *
     * @return the ApiFilter
     * @throws BadFilterException Exception when filter pattern is not matched or when any of its properties are not
     * valid.
     */
    public ApiFilter generateApiFilter(
            @NotNull String filterQuery,
            DimensionDictionary dimensionDictionary
    ) throws BadFilterException {
        LOG.trace("Filter query: {}\n\n DimensionDictionary: {}", filterQuery, dimensionDictionary);
        /*  url filter query pattern:  (dimension name)|(field name)-(operation)[?(value or comma separated values)]?
         *
         *  e.g.    locale|name-in[US,India]
         *          locale|id-eq[5]
         *
         *          dimension name: locale      locale
         *          field name:     name        id
         *          operation:      in          eq
         *          values:         US,India    5
         */
        ApiFilter inProgressApiFilter = new ApiFilter(null, null, null, new HashSet<>());

        Matcher matcher = API_FILTER_PATTERN.matcher(filterQuery);

        // if pattern match found, extract values else throw exception

        if (!matcher.matches()) {
            LOG.debug(FILTER_INVALID.logFormat(filterQuery));
            throw new BadFilterException(FILTER_INVALID.format(filterQuery));
        }

        try {
            // Extract filter dimension form the filter query.
            String filterDimensionName = matcher.group(1);
            Dimension dimension = dimensionDictionary.findByApiName(filterDimensionName);

            // If no filter dimension is found in dimension dictionary throw exception.
            if (dimension == null) {
                LOG.debug(FILTER_DIMENSION_UNDEFINED.logFormat(filterDimensionName));
                throw new BadFilterException(FILTER_DIMENSION_UNDEFINED.format(filterDimensionName));
            }
            inProgressApiFilter = inProgressApiFilter.withDimension(dimension);

            String dimensionFieldName = matcher.group(2);
            try {
                DimensionField dimensionField = dimension.getFieldByName(dimensionFieldName);
                inProgressApiFilter = inProgressApiFilter.withDimensionField(dimensionField);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_FIELD_NOT_IN_DIMENSIONS.logFormat(dimensionFieldName, filterDimensionName));
                throw new BadFilterException(
                        FILTER_FIELD_NOT_IN_DIMENSIONS.format(dimensionFieldName, filterDimensionName)
                );
            }
            String operationName = matcher.group(3);
            try {
                FilterOperation operation = DefaultFilterOperation.valueOf(operationName);
                inProgressApiFilter = inProgressApiFilter.withOperation(operation);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_OPERATOR_INVALID.logFormat(operationName));
                throw new BadFilterException(FILTER_OPERATOR_INVALID.format(operationName));
            }

            // replaceAll takes care of any leading ['s or trailing ]'s which might mess up this.values
            Set<String> values = new LinkedHashSet<>(
                    FilterTokenizer.split(
                            matcher.group(4)
                                    .replaceAll("\\[", "")
                                    .replaceAll("\\]", "")
                                    .trim()
                    )
            );
            inProgressApiFilter = inProgressApiFilter.withValues(values);
        } catch (IllegalArgumentException e) {
            LOG.debug(FILTER_ERROR.logFormat(filterQuery, e.getMessage()), e);
            throw new BadFilterException(FILTER_ERROR.format(filterQuery, e.getMessage()), e);
        }
        return inProgressApiFilter;
    }

    /**
     * Take two Api filters which differ only by value sets and union their value sets.
     *
     * @param one  The first ApiFilter
     * @param two  The second ApiFilter
     *
     * @return an ApiFilter with the union of values
     */
    public ApiFilter union(ApiFilter one, ApiFilter two) {
        if (Objects.equals(one.getDimension(), two.getDimension())
                && Objects.equals(one.getDimensionField(), two.getDimensionField())
                && Objects.equals(one.getOperation(), two.getOperation())
                ) {
            Set<String> values = Stream.concat(
                    one.getValues().stream(),
                    two.getValues().stream()
            )
                    .collect(Collectors.toSet());
            return one.withValues(values);
        }
        throw new IllegalArgumentException(String.format("Unmergable ApiFilters  '%s' and '%s'", one, two));
    }
}
