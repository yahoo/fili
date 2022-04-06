// Copyright 2022 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.util.UnmodifiableLinkedHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class ExtensibleResultSetSchema extends ResultSetSchema {

    Map<String, List<String>> additionalProperties;

    /**
     * Copy Constructor.
     *
     * @param resultSetSchema The schema to copy
     */
    public ExtensibleResultSetSchema(ResultSetSchema resultSetSchema) {
        this(
                resultSetSchema.getGranularity(),
                resultSetSchema.getColumns(),
                resultSetSchema instanceof ExtensibleResultSetSchema ?
                        ((ExtensibleResultSetSchema) resultSetSchema).additionalProperties : Collections.emptyMap()
        );
    }
    /**
     * Constructor.
     *
     * @param granularity The bucketing time grain for this schema
     * @param columns The columns in this schema
     */
    public ExtensibleResultSetSchema(
            final Granularity granularity,
            final Iterable<Column> columns
    ) {
        this(granularity, columns, Collections.emptyMap());
    }

    /**
     * Constructor.
     *
     * @param granularity The bucketing time grain for this schema
     * @param columns The columns in this schema
     * @param additionalProperties Additional properties to carry messages to the UI
     */
    public ExtensibleResultSetSchema(
            final Granularity granularity,
            final Iterable<Column> columns,
            final Map<String, List<String>> additionalProperties
    ) {
        super(granularity, columns);
        this.additionalProperties =
                UnmodifiableLinkedHashMap.of(new LinkedHashMap<>(new TreeMap<>(additionalProperties)));
    }

    public Map<String, List<String>> getAdditionalProperties() {
        return new TreeMap<>(additionalProperties);
    }

    /**
     * Store a value in the treemap.
     *
     * @param propertyName  The property key to store
     * @param propertyValue the property value to append
     *
     * @return A modified result set with this property added to the list of properties.
     */
    public ExtensibleResultSetSchema withAppendProperty(String propertyName, String propertyValue) {
        Map<String, List<String>> properties = new TreeMap<>(additionalProperties);
        properties.computeIfAbsent(propertyName, key -> new ArrayList<>()).add(propertyValue);
        return new ExtensibleResultSetSchema(getGranularity(), getColumns(), properties);
    }

    /**
     * Create a new result set with an additional final column.
     *
     * @param column the column being added
     *
     * @return the result set being constructed
     */
    public ExtensibleResultSetSchema withAddColumn(Column column) {
        LinkedHashSet<Column> columns = new LinkedHashSet<>(this.getColumns());
        columns.add(column);
        return new ExtensibleResultSetSchema(this.getGranularity(), columns, additionalProperties);
    }

    /**
     * Create a new result set with an additional final column.
     *
     * @param additionalProperties the column being added
     *
     * @return the result set being constructed
     */
    public ExtensibleResultSetSchema withAdditionalProperties(Map<String, List<String>> additionalProperties) {
        return new ExtensibleResultSetSchema(getGranularity(), getColumns(), additionalProperties);
    }

    @Override
    public String toString() {
        return "ExtensibleResultSetSchema{" +
                super.toString() +
                " additionalProperties=" + additionalProperties +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ExtensibleResultSetSchema)) { return false; }
        if (!super.equals(o)) { return false; }
        final ExtensibleResultSetSchema that = (ExtensibleResultSetSchema) o;
        return Objects.equals(getAdditionalProperties(), that.getAdditionalProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getAdditionalProperties());
    }
}
