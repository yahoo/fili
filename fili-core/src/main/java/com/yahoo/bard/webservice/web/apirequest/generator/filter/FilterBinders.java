// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_ERROR;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_FIELD_NOT_IN_DIMENSIONS;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_OPERATOR_INVALID;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.FilterOperation;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadFilterException;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

/**
 * This utility class captures default implementations for binding and validating API models for filtering requests.
 */
public class FilterBinders {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBinders.class);

    private static FilterBinders instance = new FilterBinders();

    private final ApiFilterParser filterParser;
    private final FilterFactory filterFactory;

    /**
     * Constructor.
     *
     * Use default FilterFactory.
     */
    public FilterBinders() {
        this(new RegexApiFilterParser(), new FilterFactory());
    }

    /**
     * Constructor.
     *
     * @param filterParser  Parser to convert string representations of filters into {@link FilterDefinition} instances.
     * @param filterFactory  Factory class for ApiFilters.
     */
    @Inject
    public FilterBinders(
            ApiFilterParser filterParser,
            FilterFactory filterFactory
    ) {
        this.filterParser = filterParser;
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

    public ApiFilterParser getFilterParser() {
        return filterParser;
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
            try {
                List<FilterDefinition> apiFilters = filterParser.parseApiFilterQuery(filterQuery);
                for (FilterDefinition apiFilter : apiFilters) {
                    ApiFilter newFilter;
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
            } catch (IllegalArgumentException | BadFilterException e) {
                throw new BadApiRequestException(e.getMessage(), e);
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
    public ApiFilter generateApiFilter(@NotNull String filterQuery, DimensionDictionary dimensionDictionary)
            throws BadFilterException {
        try {
            return generateApiFilter(buildFilterDefinition(filterQuery), dimensionDictionary);
        } catch (IllegalArgumentException e) {
            LOG.debug(FILTER_ERROR.logFormat(filterQuery, e.getMessage()), e);
            throw new BadFilterException(FILTER_ERROR.format(filterQuery, e.getMessage()), e);
        }
    }

    /**
     * Parses the URL filter Query and generates the ApiFilter object.
     *
     * @param filterDefinition  Parsed components of a single API query in String form
     * @param dimensionDictionary  cache containing all the valid dimension objects.
     *
     * @return the ApiFilter
     * @throws BadFilterException Exception when filter pattern is not matched or when any of its properties are not
     * valid.
     */
    public ApiFilter generateApiFilter(
            @NotNull FilterDefinition filterDefinition,
            DimensionDictionary dimensionDictionary
    ) throws BadFilterException {
        LOG.trace("Filter query: {}\n\n DimensionDictionary: {}", filterDefinition, dimensionDictionary);

        Dimension dimension;
        DimensionField dimensionField;
        FilterOperation operation;

        try {
            // Extract filter dimension form the filter query.
            dimension = dimensionDictionary.findByApiName(filterDefinition.getDimensionName());

            // If no filter dimension is found in dimension dictionary throw exception.
            if (dimension == null) {
                LOG.debug(FILTER_DIMENSION_UNDEFINED.logFormat(filterDefinition.getDimensionName()));
                throw new BadFilterException(FILTER_DIMENSION_UNDEFINED.format(filterDefinition.getDimensionName()));
            }

            try {
                dimensionField = dimension.getFieldByName(filterDefinition.getFieldName());
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_FIELD_NOT_IN_DIMENSIONS.logFormat(
                        filterDefinition.getFieldName(),
                        filterDefinition.getFieldName())
                );
                throw new BadFilterException(
                        FILTER_FIELD_NOT_IN_DIMENSIONS.format(
                                filterDefinition.getFieldName(),
                                filterDefinition.getDimensionName()
                        )
                );
            }

            try {
                operation = DefaultFilterOperation.fromString(filterDefinition.getOperationName());
            } catch (IllegalArgumentException ignored) {
                LOG.debug(FILTER_OPERATOR_INVALID.logFormat(filterDefinition.getOperationName()));
                throw new BadFilterException(FILTER_OPERATOR_INVALID.format(filterDefinition.getOperationName()));
            }


        } catch (IllegalArgumentException e) {
            LOG.debug(FILTER_ERROR.logFormat(filterDefinition, e.getMessage()), e);
            throw new BadFilterException(FILTER_ERROR.format(filterDefinition, e.getMessage()), e);
        }
        return filterFactory.buildFilter(dimension, dimensionField, operation, filterDefinition.getValues());
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
    public FilterDefinition buildFilterDefinition(@NotNull String filterQuery) throws BadFilterException {
        return filterParser.parseSingleApiFilterQuery(filterQuery);
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
