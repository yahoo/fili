// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.yahoo.bard.webservice.config.luthier.ConceptType;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Arithmetic Metric Maker produces metrics that sum, subtract, multiply or divide other metrics.
 *
 * Configuration fields:
 *
 *  * operation:  REQUIRED:  [one of: PLUS, MINUS, MULTIPLY, DIVIDE]
 */
public class ArithmeticMakerFactory implements Factory<MetricMaker> {

    protected static final String FUNCTION = "function";

    @Override
    public ArithmeticMaker build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {

        LuthierValidationUtils.validateField(configTable.get(FUNCTION), ConceptType.METRIC_MAKER, name, FUNCTION);

        String opName = configTable.get(FUNCTION).textValue();
        ArithmeticPostAggregationFunction operation = ArithmeticPostAggregationFunction.valueOf(opName);

        return new ArithmeticMaker(resourceFactories.getMetricDictionary(), operation);
    }
}
