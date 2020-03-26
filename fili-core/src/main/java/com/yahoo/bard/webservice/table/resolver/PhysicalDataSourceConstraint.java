// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.table.PhysicalTableSchema;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Data source constraint containing physical name of the columns.
 */
public class PhysicalDataSourceConstraint extends BaseDataSourceConstraint {

    private final Set<String> allColumnPhysicalNames;
    private final PhysicalTableSchema schema;

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
        this.schema = physicalTableSchema;

        this.allColumnPhysicalNames = dataSourceConstraint.getAllColumnNames().stream()
                .map(physicalTableSchema::getPhysicalColumnName)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    /**
     * Constructor. Used to service metric intersection and dimension filter logic.
     *
     * @param dataSourceConstraint  The data source constraint this constrain is based on.
     * @param physicalTableSchema  The schema of the physical table this constraint is based on. Used to resolve the
     *                             physical names of the logical columns specified in this constraint when the logical
     *                             columns are filtered.
     * @param allColumnPhysicalNames  The set of
     */
    private PhysicalDataSourceConstraint(
            @NotNull DataSourceConstraint dataSourceConstraint,
            @NotNull PhysicalTableSchema physicalTableSchema,
            @NotNull Set<String> allColumnPhysicalNames
    ) {
        super(dataSourceConstraint);
        this.schema = physicalTableSchema;
        this.allColumnPhysicalNames = Collections.unmodifiableSet(new HashSet<>(allColumnPhysicalNames));
    }

    /**
     * Getter for the all column names as physical names.
     *
     * @return the physical name of all the columns
     */
    public Set<String> getAllColumnPhysicalNames() {
        return allColumnPhysicalNames;
    }
    @Override
    public PhysicalDataSourceConstraint withDimensionFilter(Predicate<Dimension> filter) {
        DataSourceConstraint filteredConstraint = super.withDimensionFilter(filter);
        Set<String> filteredPhysicalNames = filteredConstraint.getAllColumnNames().stream()
                .map(schema::getPhysicalColumnName)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
        return new PhysicalDataSourceConstraint(filteredConstraint, schema, filteredPhysicalNames);
    }

    /**
     * Note that this intersection maintains the physical column names of logical columns that are NOT in the
     * intersection. For example, if logical column 'y' is resolved to physical column 'z' by the schema, and this
     * constraint is intersected a set of metric names that DOES NOT CONTAIN 'y', then physical column 'z' WILL be
     * maintained.
     *
     * Create a new <tt>PhysicalDataSourceConstraint</tt> instance with a new subset of metric names.
     * <p>
     * The new set of metric names will be an intersection between old metric names and
     * a user provided set of metric names
     *
     * @param metricNames  The set of metric columns that are to be intersected with metric names in
     * <tt>this BaseDataSourceConstraint</tt>
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

        return new PhysicalDataSourceConstraint(super.withMetricIntersection(metricNames), schema, resultColumnNames);
    }
}
