// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_DATASOURCE_UNION;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.table.ConstrainedTable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a Druid Union data source.
 */
public class UnionDataSource extends DataSource {

    private static final Logger LOG = LoggerFactory.getLogger(UnionDataSource.class);

    /**
     * Constructor.
     *
     * @param physicalTables  The physical tables of the data source
     */
    public UnionDataSource(Set<ConstrainedTable> physicalTables) {
        super(DefaultDataSourceType.UNION, physicalTables);
        // Check whether or not our union'd tables' dimensions match
        physicalTables.forEach(table ->
                // For each table, cycle through its dimensions and extract its physical name (i.e. expected)
                table.getDimensions().forEach(dimension -> {
                    String apiName = dimension.getApiName();
                    String expectedName = table.getPhysicalColumnName(apiName);
                    // For every other table, cycle through their dimensions and compare their names to expected
                    physicalTables.forEach(altTable -> {
                        Set<String> otherNames = altTable.getDimensions().stream()
                                .map(Dimension::getApiName)
                                .collect(Collectors.toSet());
                        String actualName = altTable.getPhysicalColumnName(apiName);
                        // If this other table contains the apiName, ensure that the physical name mapping is identical
                        if (otherNames.contains(apiName)
                                && !expectedName.equals(actualName)) {
                            LOG.error(INVALID_DATASOURCE_UNION.logFormat(apiName, expectedName, actualName));
                            throw new IllegalStateException(
                                    INVALID_DATASOURCE_UNION.format(apiName, expectedName, actualName)
                            );
                        }
                    });
                })
        );
    }

    @Override
    @JsonProperty(value = "dataSources")
    public Set<String> getNames() {
        return super.getNames();
    }

    @Override
    @JsonIgnore
    public DruidQuery<?> getQuery() {
        return null;
    }
}
