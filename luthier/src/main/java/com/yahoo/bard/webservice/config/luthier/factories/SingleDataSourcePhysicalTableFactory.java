// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.LuthierPhysicalTableParams;
import com.yahoo.bard.webservice.data.config.LuthierTableName;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.joda.time.DateTimeZone;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;

/**
 * A factory that is used by default to support Simple (non-Composite) Physical Table.
 */
abstract class SingleDataSourcePhysicalTableFactory implements Factory<ConfigPhysicalTable> {
    private static final String ENTITY_TYPE = "single data source physical table";

    /**
     * Build the parameter for the subclass of SingleDataSourceParams to use.
     *
     * @param name  Name of the LuthierTable as a String
     * @param configTable  ObjectNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  Source of the physicalTable's dependency: dimension and metadataService
     *
     * @return  a param bean that contains information need to build the PhysicalTable
     */
    LuthierPhysicalTableParams buildParams(
            String name,
            ObjectNode configTable,
            LuthierIndustrialPark resourceFactories
    ) {
        validateFields(name, configTable);

        LuthierPhysicalTableParams params = new LuthierPhysicalTableParams();
        params.tableName = new LuthierTableName(name);
        params.timeGrain = DefaultTimeGrain
                .valueOf(configTable.get("granularity").textValue().toUpperCase(Locale.US))
                .buildZonedTimeGrain(DateTimeZone.forID(configTable.get("dateTimeZone").textValue()));
        params.columns = new LinkedHashSet<>();

        JsonNode dimensionsNode = configTable.get("dimensions");
        dimensionsNode.forEach(
                node -> params.columns.add(new DimensionColumn(resourceFactories.getDimension(node.textValue())))
        );

        JsonNode metricsNode = configTable.get("metrics");
        metricsNode.forEach(
                node -> {
                    String metricName = node.textValue();
                    resourceFactories.getMetric(metricName);
                    params.columns.add(new MetricColumn(metricName));
                }
        );

        params.logicalToPhysicalColumnNames = new LinkedHashMap<>();
        configTable.get("logicalToPhysicalColumnNames").forEach(
                node -> params.logicalToPhysicalColumnNames.put(
                        node.get("logicalName").textValue(),
                        node.get("physicalName").textValue()
                )
        );
        params.metadataService = resourceFactories.getMetadataService();
        return params;
    }

    /**
     * Helper function to validate only the fields needed in the parameter build.
     *
     * @param name  name of the LuthierTable as a String
     * @param configTable  ObjectNode that points to the value of corresponding table entry in config file
     */
    protected void validateFields(String name, ObjectNode configTable) {
        LuthierValidationUtils.validateField(configTable.get("granularity"), ENTITY_TYPE, name, "granularity");
        LuthierValidationUtils.validateField(configTable.get("dateTimeZone"), ENTITY_TYPE, name, "dateTimeZone");
        LuthierValidationUtils.validateField(configTable.get("dimensions"), ENTITY_TYPE, name, "dimensions");
        LuthierValidationUtils.validateField(
                configTable.get("logicalToPhysicalColumnNames"),
                ENTITY_TYPE,
                name,
                "logicalToPhysicalColumnNames"
        );
    }
}
