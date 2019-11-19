// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.table.ConstrainedTable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DataSource base class.
 */
public abstract class DataSource {

    private final DataSourceType type;
    private final ConstrainedTable physicalTable;

    /**
     * Constructor.
     *
     * @param type  Type of the data source
     * @param physicalTable  PhysicalTables pointed to by the DataSource
     */
    public DataSource(DataSourceType type, ConstrainedTable physicalTable) {
        this.type = type;
        this.physicalTable = physicalTable;
    }

    public DataSourceType getType() {
        return type;
    }

    /**
     * Get the data source physical table.
     *
     * @return the set of physical tables for the data source
     */
    @JsonIgnore
    public ConstrainedTable getPhysicalTable() {
        return physicalTable;
    }

    /**
     * Returns a set of identifiers used by Fili to identify this data source's physical tables.
     *
     * @return The set of names used by Fili to identify this data source's physical tables
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Set<String> getNames() {
        return getPhysicalTable().getDataSourceNames().stream()
                .map(DataSourceName::asName)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toCollection(LinkedHashSet::new),
                                Collections::unmodifiableSet
                        )
                );
    }

    /**
     * Get the query that defines the data source. Empty queries become null for serialization.
     *
     * @return the serializable version of the query.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("query")
    private DruidQuery<?> getQueryForSerialization() {
        return getQuery().orElse(null);
    }

    /**
     * Get the query that defines the data source.
     * <p>
     * May be empty if the data source does not have a query.
     *
     * @return the query that this data source is generated from
     */
    @JsonIgnore
    public Optional<? extends DruidQuery<?>> getQuery() {
        return Optional.empty();
    }
}
