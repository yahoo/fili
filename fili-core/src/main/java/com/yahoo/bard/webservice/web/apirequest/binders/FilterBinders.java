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
import com.yahoo.bard.webservice.util.Incubating;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadFilterException;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.FilterOperation;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

/**
 * This utility class captures default implementations for binding and validating API models for filtering requests.
 */
@Incubating
public class FilterBinders {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBinders.class);

    protected static final String COMMA_AFTER_BRACKET_PATTERN = "(?<=]),";
    protected static final Pattern API_FILTER_PATTERN =
            Pattern.compile("([^\\|]+)\\|([^-]+)-([^\\[]+)\\[([^\\]]+)\\]?");

    private static FilterBinders instance = new FilterBinders();

    private final FilterFactory filterFactory;

    /**
     * Data object to wrap string models.
     */
    public static class FilterDefinition {
        protected String dimensionName;
        protected String fieldName;
        protected String operationName;
        protected List<String> values;
    }

    /**
     * Constructor.
     *
     * Use default FilterFactory.
     */
    public FilterBinders() {
        this(new FilterFactory());
    }

    /**
     * Constructor.
     *
     * @param filterFactory factory class for ApiFilters.
     */
    @Inject
    public FilterBinders(FilterFactory filterFactory) {
        this.filterFactory = filterFactory;
    }

    /**
     * Getter.  Useful for adding factory predicates.
     *
     * @return  The FilterFactory class.
     */
    public FilterFactory getFilterFactory() {
        return filterFactory;
    }

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
        Dimension dimension;
        DimensionField dimensionField;
        FilterOperation operation;
        FilterDefinition definition;

        try {
            definition = buildFilterDefinition(filterQuery);

            // Extract filter dimension form the filter query.
            dimension = dimensionDictionary.findByApiName(definition.dimensionName);

            // If no filter dimension is found in dimension dictionary throw exception.
            if (dimension == null) {
                LOG.debug(FILTER_DIMENSION_UNDEFINED.logFormat(definition.dimensionName));
                throw new BadFilterException(FILTER_DIMENSION_UNDEFINED.format(definition.dimensionName));
            }

            try {
                dimensionField = dimension.getFieldByName(definition.fieldName);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_FIELD_NOT_IN_DIMENSIONS.logFormat(definition.fieldName, definition.dimensionName));
                throw new BadFilterException(
                        FILTER_FIELD_NOT_IN_DIMENSIONS.format(definition.fieldName, definition.dimensionName)
                );
            }

            try {
                operation = DefaultFilterOperation.fromString(definition.operationName);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_OPERATOR_INVALID.logFormat(definition.operationName));
                throw new BadFilterException(FILTER_OPERATOR_INVALID.format(definition.operationName));
            }


        } catch (IllegalArgumentException e) {
            LOG.debug(FILTER_ERROR.logFormat(filterQuery, e.getMessage()), e);
            throw new BadFilterException(FILTER_ERROR.format(filterQuery, e.getMessage()), e);
        }
        return filterFactory.buildFilter(dimension, dimensionField, operation, definition.values);
    }

    /**
     * Capture the unbound parameters for binding and validating filters.
     *
     * @param filterQuery  The raw filterQuery as provided by the URI.
     *
     * @return  An object describing the string components from the request URI.
     *
     * @throws BadFilterException Exception when filter pattern is not matched or when any of its properties are not
     * valid.
     */
    public FilterDefinition buildFilterDefinition(String filterQuery) throws BadFilterException {
        FilterDefinition filterDefinition = new FilterDefinition();

        Matcher matcher = API_FILTER_PATTERN.matcher(filterQuery);

        if (!matcher.matches()) {
            LOG.debug(FILTER_INVALID.logFormat(filterQuery));
            throw new BadFilterException(FILTER_INVALID.format(filterQuery));
        }

        filterDefinition.dimensionName = matcher.group(1);
        filterDefinition.fieldName = matcher.group(2);
        filterDefinition.operationName = matcher.group(3);
        // replaceAll takes care of any leading ['s or trailing ]'s which might mess up this.values
        List<String> values = new ArrayList<>(
                FilterTokenizer.split(
                        matcher.group(4)
                                .replaceAll("\\[", "")
                                .replaceAll("\\]", "")
                                .trim()
                )
        );
        filterDefinition.values = values;
        return filterDefinition;
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

    public static FilterBinders getInstance() {
        return instance;
    }

    public static void setInstance(final FilterBinders instance) {
        FilterBinders.instance = instance;
    }
}
