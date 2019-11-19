// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.table.PhysicalTableSchema;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Data source constraint containing physical name of the columns.
 */
public class PhysicalDataSourceConstraint extends DataSourceConstraint {

    private final Set<String> allColumnPhysicalNames;

    /**
     * Constructor.
     *
     * @param dataSourceConstraint  Data source constraint containing all the column names as logical names
     * @param physicalTableSchema  A map from logical column name to physical column names
     */
    public PhysicalDataSourceConstraint(
            @NotNull DataSourceConstraint dataSourceConstraint,
            @NotNull PhysicalTableSchema physicalTableSchema
    ) {
        super(dataSourceConstraint);

        this.allColumnPhysicalNames = dataSourceConstraint.getAllColumnNames().stream()
                .map(physicalTableSchema::getPhysicalColumnName)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    /**
     * Constructor, use with care, beware of possible inconsistent between underlying dimension and metrics property
     * and allColumnPhysicalName.
     *
     * @param dataSourceConstraint  Data source constraint containing all the column names as logical names
     * @param allColumnPhysicalNames  The physical names of the columns
     */
    private PhysicalDataSourceConstraint(
            @NotNull DataSourceConstraint dataSourceConstraint,
            @NotNull Set<String> allColumnPhysicalNames
    ) {
        super(dataSourceConstraint);
        this.allColumnPhysicalNames = allColumnPhysicalNames;
    }

    /**
     * Getter for the all column names as physical names.
     *
     * @return the physical name of all the columns
     */
    public Set<String> getAllColumnPhysicalNames() {
        return allColumnPhysicalNames;
    }

    /**
     * Create a new <tt>PhysicalDataSourceConstraint</tt> instance with a new subset of metric names.
     * <p>
     * The new set of metric names will be an intersection between old metric names and
     * a user provided set of metric names
     *
     * @param metricNames  The set of metric columns that are to be intersected with metric names in
     * <tt>this DataSourceConstraint</tt>
     *
     * @return the new <tt>PhysicalDataSourceConstraint</tt> instance with a new subset of metric names
     */
    @Override
    public PhysicalDataSourceConstraint withMetricIntersection(Set<String> metricNames) {
        Set<String> nonIntersectingMetric = getMetricNames().stream()
                .filter(metricName -> !metricNames.contains(metricName))
                .collect(Collectors.toSet());

        Set<String> resultColumnNames = this.allColumnPhysicalNames.stream()
                .filter(name -> !nonIntersectingMetric.contains(name))
                .collect(Collectors.toSet());

        return new PhysicalDataSourceConstraint(super.withMetricIntersection(metricNames), resultColumnNames);
    }
}
