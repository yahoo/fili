package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public interface DataSourceConstraint {

    /**
     * // TODO document returns base class
     *
     * Build a constraint which should not filter away any part of a given table.
     *
     * @param table  The table whose dimensions and metrics are to be queried
     *
     * @return a constraint which should provide no restrictions
     */
    static DataSourceConstraint unconstrained(PhysicalTable table) {
        return new BaseDataSourceConstraint(
                table.getDimensions(),
                Collections.emptySet(),
                Collections.emptySet(),
                table.getSchema().getMetricColumnNames(),
                table.getDimensions(),
                table.getDimensions().stream()
                        .map(Dimension::getApiName)
                        .collect(Collectors.toSet()),
                table.getSchema().getColumnNames(),
                new ApiFilters(Collections.emptyMap())
        );
    }

    Set<Dimension> getRequestDimensions();

    Set<Dimension> getFilterDimensions();

    Set<Dimension> getMetricDimensions();

    Set<String> getMetricNames();

    Set<Dimension> getAllDimensions();

    Set<String> getAllDimensionNames();

    Set<String> getAllColumnNames();

    ApiFilters getApiFilters();

    DataSourceConstraint withMetricIntersection(Set<String> metricNames);
}
