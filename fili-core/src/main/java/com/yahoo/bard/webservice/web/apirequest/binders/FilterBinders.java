// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_NOT_IN_TABLE;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

/**
 * This utility class captures default implementations for binding and validating API models for filtering requests.
 */
@Incubating
public class FilterBinders implements FilterGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBinders.class);

    // TODO what is the use of FilterFactory? Can it be removed? it looks like it is unused. I think the
    // ApiFilterGenerator interface is sufficient abstraction on building api filters.
    private final FilterFactory filterFactory;

    // TODO if FilterFactory can be removed we don't need to instance control this class.
    private static FilterBinders instance = new FilterBinders();


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
     * @return The FilterFactory class.
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
    @Override
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
            List<String> apiFilters =
                    Arrays.asList(FilterGenerationUtils.COMMA_AFTER_BRACKET_PATTERN.split(filterQuery));
            for (String apiFilter : apiFilters) {
                ApiFilter newFilter;
                try {
                    FilterGenerationUtils.FilterComponents filterComponents
                            = FilterGenerationUtils.buildFilterComponents(apiFilter, dimensionDictionary);
                    newFilter = FilterGenerationUtils.DEFAULT_FILTER_FACTORY.buildFilter(
                            filterComponents.dimension,
                            filterComponents.dimensionField,
                            filterComponents.operation,
                            filterComponents.values
                    );

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
     * @deprecated prefer the utility method
     * {@link FilterGenerationUtils#generateApiFilter(String, DimensionDictionary)} instead of this one.
     */
    @Deprecated
    public ApiFilter generateApiFilter(
            @NotNull String filterQuery,
            DimensionDictionary dimensionDictionary
    ) {
        return FilterGenerationUtils.generateApiFilter(filterQuery, dimensionDictionary);
    }

    @Override
    public void validateApiFilters(
            String filterQuery,
            ApiFilters apiFilters,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    ) {
        // no default validation
    }

    public static FilterBinders getInstance() {
        return instance;
    }

    public static void setInstance(final FilterBinders instance) {
        FilterBinders.instance = instance;
    }
}
