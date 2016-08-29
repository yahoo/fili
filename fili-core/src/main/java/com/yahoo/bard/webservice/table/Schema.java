// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.util.Utils;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Schema.
 */
public class Schema {

    private final Granularity granularity;

    protected LinkedHashSet<Column> columns;

    /**
     * Constructor.
     *
     * @param granularity  The granularity of the table
     */
    public Schema(@NotNull Granularity granularity) {
        this.granularity = granularity;
        this.columns = new LinkedHashSet<>();
    }

    /**
     * Getter for timeGrain.
     *
     * @return timeGrain
     */
    public Granularity getGranularity() {
        return this.granularity;
    }

    public Set<Column> getColumns() {
        return columns;
    }

    /**
     * Getter for set of columns by sub-type.
     *
     * @param columnClass  The class of columns to to search
     * @param <T> sub-type of Column to return
     *
     * @return Set of Columns
     */
    public <T extends Column> Set<T> getColumns(Class<T> columnClass) {
        return Utils.getSubsetByType(getColumns(), columnClass);
    }

    /**
     * Getter for column by name.
     *
     * @param columnName  Name of the column
     *
     * @return Column having name columnName
     */
    public Column getColumn(String columnName) {
        for (Column column : getColumns()) {
            if (columnName.equals(column.getName())) {
                return column;
            }
        }
        return null;
    }

    /**
     * Get a column by its name.
     *
     * @param columnName  Name of the column
     * @param columnClass  sub class
     * @param <T> sub class type
     *
     * @return Column having name columnName
     */
    public <T extends Column> T getColumn(String columnName, Class<T> columnClass) {
        for (T column: Utils.getSubsetByType(getColumns(), columnClass)) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    /**
     * Method to add a column.
     *
     * @param columnToAdd  The column to add
     *
     * @return set of columns which has the newly added column
     */
    public Boolean addColumn(Column columnToAdd) {
        return this.columns.add(columnToAdd);
    }

    /**
     * Method to remove a column.
     *
     * @param columnToRemove  The column to remove
     *
     * @return set of columns after removing the specified column
     */
    public Boolean removeColumn(Column columnToRemove) {
        return this.columns.remove(columnToRemove);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        Schema schema = (Schema) o;
        return
                Objects.equals(columns, schema.columns) &&
                Objects.equals(granularity, schema.granularity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(granularity, columns);
    }

    @Override
    public String toString() {
        return columns.toString();
    }
}
