// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.LuthierTableName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.SingleDataSourcePhysicalTable;

import org.joda.time.DateTimeZone;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A factory that is used by default to support Simple (non-Composite) Physical Table.
 */
public abstract class SingleDataSourcePhysicalTableFactory implements Factory<ConfigPhysicalTable> {
    // public static final String DEFAULT_FIELD_NAME_ERROR =
    //         "Base physical table '%s': defaultField name '%s' not found in fields '%s'";
    private static final String PHYSICAL_TABLE = "single data source physical table";

    public static class SingleDataSourceParams {
        TableName tableName;
        ZonedTimeGrain timeGrain;
        Set<Column> columns;
        Map<String, String> logicalToPhysicalColumnNames;
        DataSourceMetadataService metadataService;
    }


    protected SingleDataSourceParams buildParams(
            String name,
            ObjectNode configTable,
            LuthierIndustrialPark resourceFactories
    ) {
        SingleDataSourceParams params = new SingleDataSourceParams();
        params.tableName = new LuthierTableName(name);
        // TODO: Time grain not tested yet
        LuthierValidationUtils.validateField(configTable.get("granularity"), PHYSICAL_TABLE, name, "granularity");
        LuthierValidationUtils.validateField(configTable.get("dateTimeZone"), PHYSICAL_TABLE, name, "dateTimeZone");
        params.timeGrain = DefaultTimeGrain.valueOf(
                configTable.get("granularity").textValue().toUpperCase(Locale.US)
        ).buildZonedTimeGrain(DateTimeZone.forID(configTable.get("dateTimeZone").textValue()));
        // TODO: columns not tested yet
        params.columns = new LinkedHashSet<>();
        LuthierValidationUtils.validateField(
                configTable.get("dimensions"),
                PHYSICAL_TABLE,
                name,
                "dimensions"
        );
        JsonNode dimensionsNode = configTable.get("dimensions");
        dimensionsNode.forEach(
                node -> params.columns.add(new DimensionColumn(resourceFactories.getDimension(node.textValue())))
        );
        // TODO: think about using LogicalMetricColumn
        JsonNode metricsNode = configTable.get("metrics");
        metricsNode.forEach(
                node -> params.columns.add(new MetricColumn(node.textValue()))
        );
        // TODO: logicalToPhysicalColumnNames not tested yet
        params.logicalToPhysicalColumnNames = new LinkedHashMap<>();
        LuthierValidationUtils.validateField(
                configTable.get("logicalToPhysicalColumnNames"),
                PHYSICAL_TABLE,
                name,
                "searchProvider"
        );
        JsonNode columnNameMapNode = configTable.get("logicalToPhysicalColumnNames");
        columnNameMapNode.forEach(
                node -> params.logicalToPhysicalColumnNames.put(
                        node.get("logicalName").textValue(),
                        node.get("physicalName").textValue()
                )
        );
        params.metadataService = resourceFactories.getMetadataService();
        return params;
    }

}
