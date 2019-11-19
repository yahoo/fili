// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import static com.yahoo.bard.webservice.application.DruidDimensionValueLoader.DRUID_DIM_LOADER_ROW_LIMIT;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery;
import com.yahoo.bard.webservice.sql.SqlBackedClient;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.SqlPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.Utils;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Years;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The SqlDimensionValueLoader sends values to the configured sql backend to load dimension values into the dimension
 * cache.
 */
public class SqlDimensionValueLoader implements DimensionValueLoader {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final Integer ROW_LIMIT = SYSTEM_CONFIG.getIntProperty(DRUID_DIM_LOADER_ROW_LIMIT, 1000);
    private static final Interval INTERVAL = new Interval(Years.years(10), DateTime.now());
    private FailureCallback failureCallback;
    private final Set<Dimension> dimensions;
    private final Set<DataSource> dataSources;
    private final SqlBackedClient sqlBackedClient;

    /**
     * SqlDimensionValueLoader fetches data from Sql and adds it to the dimension cache.
     * The dimensions to be loaded can be passed in as a parameter.
     *
     * @param physicalTableDictionary  The physical tables
     * @param dimensionDictionary  The dimension dictionary to load dimensions from.
     * @param dimensionsToLoad  The dimensions to be loaded.
     * @param sqlBackedClient  The sql backed client.
     */
    public SqlDimensionValueLoader(
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary,
            List<String> dimensionsToLoad,
            SqlBackedClient sqlBackedClient
    ) {
        this.dimensions = dimensionsToLoad.stream()
                .map(dimensionDictionary::findByApiName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        this.dataSources = physicalTableDictionary.values().stream()
                .map(table -> table.withConstraint(DataSourceConstraint.unconstrained(table)))
                .map(TableDataSource::new)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        this.sqlBackedClient = sqlBackedClient;
    }

    @Override
    public void query(Dimension dimension, DataSource dataSource) {
        SuccessCallback successCallback = buildSuccessCallback(dimension);
        SqlPhysicalTable sqlTable = (SqlPhysicalTable) dataSource.getPhysicalTable().getSourceTable();

        GroupByQuery groupByQuery = new GroupByQuery(
                dataSource,
                AllGranularity.INSTANCE,
                Collections.singletonList(dimension),
                null,
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(INTERVAL),
                new LimitSpec(Utils.asLinkedHashSet(), Optional.of(ROW_LIMIT))
        );

        sqlBackedClient.executeQuery(groupByQuery, successCallback, failureCallback);
    }

    /**
     * Reads the results of a Druid GroupBy Query to find dimension values.
     *
     * @param dimension  The dimension to load values for.
     *
     * @return the callback to load dimension values.
     */
    private SuccessCallback buildSuccessCallback(Dimension dimension) {
        return rootNode -> {
            rootNode.forEach(row -> {
                JsonNode eventRow = row.get("event");

                String dimensionValue = eventRow.get(dimension.getApiName()).asText();

                if (dimension.findDimensionRowByKeyValue(dimensionValue) == null) {
                    DimensionRow dimensionRow = dimension.createEmptyDimensionRow(dimensionValue);
                    updateDimensionWithValue(dimension, dimensionRow);
                }
            });

            updateDimension(dimension);
        };
    }

    @Override
    public void setErrorCallback(HttpErrorCallback errorCallback) {
        // Nothing to do, Sql won't encounter http errors.
    }

    @Override
    public void setFailureCallback(FailureCallback failureCallback) {
        this.failureCallback = failureCallback;
    }

    @Override
    public Set<Dimension> getDimensions() {
        return dimensions;
    }

    @Override
    public Set<DataSource> getDataSources() {
        return dataSources;
    }
}
