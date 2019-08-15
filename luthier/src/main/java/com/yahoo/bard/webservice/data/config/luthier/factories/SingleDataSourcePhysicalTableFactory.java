// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories;

import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.exceptions.LuthierFactoryException;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.luthier.table.LuthierPhysicalTableParams;
import com.yahoo.bard.webservice.data.config.luthier.names.LuthierTableName;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.util.GranularityParseException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.joda.time.DateTimeZone;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

/**
 * A factory that is used by default to support Simple (non-Composite) Physical Table.
 */
public abstract class SingleDataSourcePhysicalTableFactory implements Factory<ConfigPhysicalTable> {
    private static final String ENTITY_TYPE = "single data source physical table";
    private static final String GRANULARITY_DICTIONARY_MISSING = "granularityDictionary missing from the " +
            "LuthierIndustrialPark. Will not build " + ENTITY_TYPE + ": %s.";
    private static final String GRANULARITY_PARSING_ERROR =
            "granularity: %s does not exist in the granularityDictionary, when parsing %s";
    private static final String GRANULARITY_TYPE_ERROR =
            "%s '%s' expects a ZonedTimeGrain, but found another kind of Granularity. Did you use an AllGranularity?";
    private static final String GRANULARITY = "granularity";
    private static final String DATE_TIME_ZONE = "dateTimeZone";
    private static final String DIMENSIONS = "dimensions";
    private static final String LOGICAL_TO_PHYSICAL_COLUMN_NAMES = "logicalToPhysicalColumnNames";
    private static final String METRICS = "metrics";

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
        if (resourceFactories.getGranularityParser() == null) {
            throw new LuthierFactoryException(String.format(GRANULARITY_DICTIONARY_MISSING, name));
        }
        LuthierValidationUtils.validateFields(
                configTable,
                ENTITY_TYPE,
                name,
                GRANULARITY, DATE_TIME_ZONE, DIMENSIONS, LOGICAL_TO_PHYSICAL_COLUMN_NAMES, METRICS
        );

        LuthierPhysicalTableParams params = new LuthierPhysicalTableParams();
        params.tableName = new LuthierTableName(name);

        DateTimeZone dateTimeZone = DateTimeZone.forID(configTable.get(DATE_TIME_ZONE).textValue());
        try {
            Granularity granularity = resourceFactories.getGranularityParser().parseGranularity(
                    configTable.get(GRANULARITY).textValue(),
                    dateTimeZone
            );
            if (!(granularity instanceof ZonedTimeGrain)) {
                throw new LuthierFactoryException(
                        String.format(GRANULARITY_TYPE_ERROR, ENTITY_TYPE, name)
                );
            }
            params.timeGrain = (ZonedTimeGrain) granularity;
        } catch (GranularityParseException e) {
            throw new LuthierFactoryException(
                    String.format(GRANULARITY_PARSING_ERROR, name, ENTITY_TYPE),
                    e
            );
        }

        params.columns = new LinkedHashSet<>();

        JsonNode dimensionsNode = configTable.get(DIMENSIONS);
        dimensionsNode.forEach(
                node -> params.columns.add(new DimensionColumn(resourceFactories.getDimension(node.textValue())))
        );

        JsonNode metricsNode = configTable.get(METRICS);
        metricsNode.forEach(
                node -> {
                    String metricName = node.textValue();
                    resourceFactories.getMetric(metricName);
                    params.columns.add(new MetricColumn(metricName));
                }
        );

        params.logicalToPhysicalColumnNames = new LinkedHashMap<>();
        configTable.get(LOGICAL_TO_PHYSICAL_COLUMN_NAMES).forEach(
                node -> params.logicalToPhysicalColumnNames.put(
                        node.get("logicalName").textValue(),
                        node.get("physicalName").textValue()
                )
        );
        params.metadataService = resourceFactories.getMetadataService();
        return params;
    }
}
