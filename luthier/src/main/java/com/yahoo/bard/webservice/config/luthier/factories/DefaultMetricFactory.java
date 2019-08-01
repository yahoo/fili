// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A Factory that supports all aggregation metrics, including but not limited to: longSum, doubleMax, longMin, etc.
 */
public class DefaultMetricFactory implements Factory<LogicalMetric> {
    private static final String ENTITY_TYPE = "AggregationMetric";
    private static final String MAKER = "maker";
    private static final String LONG_NAME = "longName";
    private static final String CATEGORY = "category";
    private static final String DESCRIPTION = "description";
    private static final String DATA_TYPE = "dataType";
    private static final String DEPENDENCY_METRIC_NAMES = "dependencyMetricNames";

    @Override
    public LogicalMetric build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        validateFields(
                name,
                configTable,
                Arrays.asList(MAKER, LONG_NAME, CATEGORY, DESCRIPTION, DEPENDENCY_METRIC_NAMES)
        );
        MetricMaker maker = resourceFactories.getMetricMaker(configTable.get(MAKER).textValue());
        LogicalMetricInfo metricInfo = configTable.get(DATA_TYPE) == null ?
                new LogicalMetricInfo(
                        name,
                        configTable.get(LONG_NAME).textValue(),
                        configTable.get(CATEGORY).textValue(),
                        configTable.get(DESCRIPTION).textValue()
                )
                : new LogicalMetricInfo(
                        name,
                        configTable.get(LONG_NAME).textValue(),
                        configTable.get(CATEGORY).textValue(),
                        configTable.get(DESCRIPTION).textValue(),
                        configTable.get(DATA_TYPE).textValue()
                );
        List<String> dependencyMetricNames = StreamSupport.stream(
                configTable.get(DEPENDENCY_METRIC_NAMES).spliterator(),
                false
        )
                .map(JsonNode::textValue)
                .collect(Collectors.toList());
        return maker.make(metricInfo, dependencyMetricNames);
    }
    /**
     * Helper function to validate only the fields needed in the parameter build.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  ObjectNode that points to the value of corresponding table entry in config file
     * @param fieldNames  the list of field names we want to validate existence in this configTable
     */
    private void validateFields(String name, ObjectNode configTable, List<String> fieldNames) {
        fieldNames.forEach(
                fieldName -> LuthierValidationUtils.validateField(
                        configTable.get(fieldName),
                        ENTITY_TYPE,
                        name,
                        fieldName)
        );
    }
}
