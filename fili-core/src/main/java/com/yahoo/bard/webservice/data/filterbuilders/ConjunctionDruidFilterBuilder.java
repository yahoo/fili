// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.filterbuilders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOO_MANY_DRUID_FILTERS;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.data.dimension.impl.ExtractionFunctionDimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.model.filter.AndFilter;
import com.yahoo.bard.webservice.druid.model.filter.ExtractionFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter;
import com.yahoo.bard.webservice.exception.TooManyDruidFiltersException;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A ConjunctionDruidFilterBuilder builds a Druid filter by taking the conjunction of filter clauses, one for
 * each dimension being filtered on.
 */
public abstract class ConjunctionDruidFilterBuilder implements DruidFilterBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ConjunctionDruidFilterBuilder.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final int DEFAULT_MAX_NUM_DRUID_FILTERS = 10000;
    private static final int MAX_NUM_DRUID_FILTERS = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("max_num_druid_filters"),
            DEFAULT_MAX_NUM_DRUID_FILTERS
    );

    @Override
    public Filter buildFilters(Map<Dimension, Set<ApiFilter>> filterMap) throws DimensionRowNotFoundException {
        LOG.trace("Building filters using filter map: {}", filterMap);

        // return null when no filter is specified in the API
        if (filterMap.isEmpty()) {
            return null;
        }

        List<Filter> dimensionFilters = new ArrayList<>(filterMap.size());
        for (Map.Entry<Dimension, Set<ApiFilter>> entry : filterMap.entrySet()) {
            dimensionFilters.add(buildDimensionFilter(entry.getKey(), entry.getValue()));
        }

        // for a single filter just return the entry and not a collection containing one entry
        if (dimensionFilters.size() == 1) {
            return dimensionFilters.get(0);
        }

        AndFilter newFilter = new AndFilter(dimensionFilters);

        if (newFilter.getFields().size() > MAX_NUM_DRUID_FILTERS) {
            LOG.error(TOO_MANY_DRUID_FILTERS.logFormat());
            throw new TooManyDruidFiltersException(TOO_MANY_DRUID_FILTERS.format());
        }

        LOG.trace("Filter: {}", newFilter);
        return newFilter;
    }

    /**
     * Take the conjunction of all the filters on a single dimension.
     *
     * @param dimension  Dimension for the filters
     * @param filters  All filters belonging to that dimension
     *
     * @return A druid query filter object representing the filtering on a given dimension
     *
     * @throws DimensionRowNotFoundException if we attempt to filter a dimension without dimension rows
     */
    protected abstract Filter buildDimensionFilter(Dimension dimension, Set<ApiFilter> filters)
            throws DimensionRowNotFoundException;

    /**
     * Resolves a set of ApiFilters into a list of dimension rows that need to be filtered in Druid.
     *
     * @param dimension  The dimension being filtered
     * @param filters  The filters being applied to the {@code dimension}
     *
     * @return A list of dimension rows that Druid needs to filter on
     *
     * @throws DimensionRowNotFoundException if the filters filter out all dimension rows
     */
    protected Set<DimensionRow> getFilteredDimensionRows(Dimension dimension, Set<ApiFilter> filters)
            throws DimensionRowNotFoundException {
        Set<DimensionRow> rows = dimension.getSearchProvider().findFilteredDimensionRows(filters);

        if (rows.isEmpty()) {
            String msg = ErrorMessageFormat.DIMENSION_ROWS_NOT_FOUND.format(dimension.getApiName(), filters);
            LOG.debug(msg);
            throw new DimensionRowNotFoundException(msg);
        }

        return rows;
    }

    /**
     * Builds a list of Druid selector or extraction filters.
     *
     * @param dimension  The dimension to build the list of Druid selector filters from
     * @param rows  The set of dimension rows that need selector filters built around
     *
     * @return a list of Druid selector filters
     */
    protected List<Filter> buildSelectorFilters(Dimension dimension, Set<DimensionRow> rows) {

        Function<DimensionRow, Filter> filterBuilder = row -> new SelectorFilter(
                dimension,
                row.get(dimension.getKey())
        );

        if (dimension instanceof ExtractionFunctionDimension) {

            Optional<ExtractionFunction> extractionFunction = ((ExtractionFunctionDimension) dimension)
                    .getExtractionFunction();
            if (extractionFunction.isPresent()) {
                filterBuilder = row -> new ExtractionFilter(
                        dimension,
                        row.get(dimension.getKey()),
                        extractionFunction.get()
                );
            }
        }

        final Function<DimensionRow, Filter> finalFilterBuilder = filterBuilder;

        return rows.stream()
                    .map(row -> finalFilterBuilder.apply(row))
                    .collect(Collectors.toList());
    }
}
