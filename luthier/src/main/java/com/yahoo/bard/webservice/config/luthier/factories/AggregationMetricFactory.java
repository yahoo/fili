// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;

import java.util.Collections;

/**
 * A Factory that supports all aggregation metrics, including but not limited to: longSum, doubleMax, longMin, etc.
 */
public class AggregationMetricFactory implements Factory<LogicalMetric> {
    private static final String ENTITY_TYPE = "AggregationMetric";

    @Override
    public LogicalMetric build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        validateFields(name, configTable);
        MetricMaker maker = resourceFactories.getMetricMaker(configTable.get("maker").textValue());
        LogicalMetricInfo metricInfo = configTable.get("dataType") == null ?
                new LogicalMetricInfo(
                        name,
                        configTable.get("longName").textValue(),
                        configTable.get("category").textValue(),
                        configTable.get("description").textValue()
                )
                : new LogicalMetricInfo(
                        name,
                        configTable.get("longName").textValue(),
                        configTable.get("category").textValue(),
                        configTable.get("description").textValue(),
                        configTable.get("dataType").textValue()
                );
        return maker.make(metricInfo, Collections.singletonList(configTable.get("druidMetric").textValue()));
    }
    /**
     * Helper function to validate only the fields needed in the parameter build.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  ObjectNode that points to the value of corresponding table entry in config file
     */
    private void validateFields(String name, ObjectNode configTable) {
        LuthierValidationUtils.validateField(configTable.get("maker"), ENTITY_TYPE, name, "maker");
        LuthierValidationUtils.validateField(configTable.get("longName"), ENTITY_TYPE, name, "longName");
        LuthierValidationUtils.validateField(configTable.get("category"), ENTITY_TYPE, name, "category");
        LuthierValidationUtils.validateField(configTable.get("description"), ENTITY_TYPE, name, "description");
        LuthierValidationUtils.validateField(configTable.get("druidMetric"), ENTITY_TYPE, name, "druidMetric");
    }
}
