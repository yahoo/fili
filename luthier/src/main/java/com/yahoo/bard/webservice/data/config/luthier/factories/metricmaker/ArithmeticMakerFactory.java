// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.ConceptType;
import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction;

/**
 * Arithmetic Metric Maker produces metrics that sum, subtract, multiply or divide other metrics.
 *
 * Configuration fields:
 *
 *  * operation:  REQUIRED:  [one of: PLUS, MINUS, MULTIPLY, DIVIDE]
 */
public class ArithmeticMakerFactory implements Factory<MetricMaker> {

    protected static final String OPERATION = "operation";

    @Override
    public ArithmeticMaker build(String name, LuthierConfigNode configTable, LuthierIndustrialPark resourceFactories) {

        LuthierValidationUtils.validateField(configTable.get(OPERATION), ConceptType.METRIC_MAKER, name, OPERATION);

        String opName = configTable.get(OPERATION).textValue();
        ArithmeticPostAggregationFunction operation = ArithmeticPostAggregationFunction.valueOf(opName);

        return new ArithmeticMaker(resourceFactories.getMetricDictionary(), operation);
    }
}
