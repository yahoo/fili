// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTableSchema;

import java.util.Set;
import java.util.stream.Collectors;

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
            DataSourceConstraint dataSourceConstraint,
            PhysicalTableSchema physicalTableSchema
    ) {
        super(dataSourceConstraint);

        Set<String> schemaColumnNames = physicalTableSchema.getColumnNames();

        this.allColumnPhysicalNames = dataSourceConstraint.getAllColumnNames().stream()
                .filter(schemaColumnNames::contains)
                .map(physicalTableSchema::getPhysicalColumnName)
                .collect(Collectors.toSet());
    }

    /**
     * Private Constructor that union columns in constraint and schema, use buildWithSchemaUnion to invoke.
     *
     * @param physicalTableSchema  A map from logical column name to physical column names
     * @param dataSourceConstraint  Data source constraint containing all the column names as logical names
     */
    private PhysicalDataSourceConstraint(
            PhysicalTableSchema physicalTableSchema,
            DataSourceConstraint dataSourceConstraint
    ) {
        super(dataSourceConstraint);

        Set<String> schemaColumnNames = physicalTableSchema.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toSet());

        this.allColumnPhysicalNames = schemaColumnNames.stream()
                .map(physicalTableSchema::getPhysicalColumnName)
                .collect(Collectors.toSet());
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
     * Builds a physical data source constraint with the union of columns in schema.
     *
     * @param dataSourceConstraint  Data source constraint containing all the column names as logical names
     * @param physicalTableSchema  A map from logical column name to physical column names
     *
     * @return a new physical data source constraint with all columns in schema
     */
    public static PhysicalDataSourceConstraint buildWithSchemaUnion(
            DataSourceConstraint dataSourceConstraint,
            PhysicalTableSchema physicalTableSchema
    ) {
        return new PhysicalDataSourceConstraint(physicalTableSchema, dataSourceConstraint);
    }
}
