// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.logicalmetric;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.exceptions.LuthierFactoryException;

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
    private static final String ILLEGAL_ARG_EXCEPTION = "Unexpected argument found when building "
            + ENTITY_TYPE + " %s.";
    private static final String UNSUPPORTED_OPS_EXCEPTION = "Certain operation when building "
            + ENTITY_TYPE + " %s is not yet supported.";

    @Override
    public LogicalMetric build(String name, LuthierConfigNode configTable, LuthierIndustrialPark resourceFactories) {
        LuthierValidationUtils.validateFields(
                configTable,
                ENTITY_TYPE,
                name,
                MAKER, LONG_NAME, CATEGORY, DESCRIPTION, DEPENDENCY_METRIC_NAMES
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
                .map(LuthierConfigNode::textValue)
                .collect(Collectors.toList());
        try {
            return maker.make(metricInfo, dependencyMetricNames);
        } catch (IllegalArgumentException e) {
            throw new LuthierFactoryException(String.format(ILLEGAL_ARG_EXCEPTION, name), e);
        } catch (UnsupportedOperationException e) {
            throw new LuthierFactoryException(String.format(UNSUPPORTED_OPS_EXCEPTION, name), e);
        }
    }
}
