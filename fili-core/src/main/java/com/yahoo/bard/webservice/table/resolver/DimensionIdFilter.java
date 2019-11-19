// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of {@link DataSourceFilter} which uses dimensionApiFilters to match partition data sources.
 * The matching algorithm requires that for each dimension specified, the query filters must match one of the
 * corresponding values.
 */
public class DimensionIdFilter implements DataSourceFilter {

    private final Map<Dimension, ApiFilter> dimensionKeySelectFilters;

    /**
     * Constructor.
     *
     * @param dimensionMappingValues  The map of dimension to sets of dimension values which map this table.
     */
    public DimensionIdFilter(Map<Dimension, Set<String>> dimensionMappingValues) {
        dimensionKeySelectFilters = dimensionMappingValues.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(
                        entry.getKey(),
                        new ApiFilter(
                                entry.getKey(),
                                entry.getKey().getKey(),
                                DefaultFilterOperation.in,
                                entry.getValue()
                        )
                ))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Check if for a given set of request filters, adding in the filters for this `DataSourceFilter` there is a
     * corresponding row.
     * This method should never be called with a dimension that isn't a key in {@link #dimensionKeySelectFilters}
     *
     * @param dimension  The dimension whose rows are being tested on.
     * @param constraintFilters  The api filters from the constraint
     *
     * @return true if for this dimension there are rows matching the query filters AND the embedded filters.
     */
    protected boolean anyRowsMatch(@NotNull Dimension dimension, @NotNull Set<ApiFilter> constraintFilters) {
        if (!dimensionKeySelectFilters.containsKey(dimension)) {
            throw new IllegalArgumentException(
                    "Any rows match should only be called with dimensions defined on this filter."
            );
        }
        Set<ApiFilter> combinedFilters = StreamUtils.append(
                constraintFilters,
                dimensionKeySelectFilters.get(dimension)
        );
        return dimension.getSearchProvider().hasAnyRows(combinedFilters);
    }

    /**
     * Test whether constraints for a particular dimension are missing, empty or match rows.
     *
     * @param dimension  The dimension for filtering
     * @param constraintMap  The map of filters from the constraint object
     *
     * @return true if the constraintMap doesn't constraint this dimension or if the constraint returns rows
     */
    private boolean emptyConstraintOrAnyRows(Dimension dimension, Map<Dimension, Set<ApiFilter>> constraintMap) {
        return !constraintMap.containsKey(dimension) ||
                constraintMap.get(dimension).isEmpty() ||
                anyRowsMatch(dimension, constraintMap.get(dimension));
    }

    @Override
    public Boolean apply(DataSourceConstraint constraint) {
        Map<Dimension, Set<ApiFilter>> constraintMap = constraint.getApiFilters();

        return dimensionKeySelectFilters.keySet()
                .stream()
                .allMatch(dimension -> emptyConstraintOrAnyRows(dimension, constraintMap));
    }

    @Override
    public int hashCode() {
        return dimensionKeySelectFilters.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof DimensionIdFilter) {
            return Objects.equals(dimensionKeySelectFilters, ((DimensionIdFilter) obj).dimensionKeySelectFilters);
        }
        return false;
    }
}
