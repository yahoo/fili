// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.ConceptType;
import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.exceptions.LuthierFactoryException;
import com.yahoo.bard.webservice.util.GranularityParseException;

/**
 * Factory that supports AggregationAverageMaker.
 */
public class AggregationAverageMakerFactory implements Factory<MetricMaker> {
    private static final String AGGREGATION_AVG_MAKER = "AggregationAverageMaker";
    private static final String INNER_GRAIN = "innerGrain";
    private static final String GRAIN_NAME_NOT_EXPECTED = "granularity name '%s' is not recognized by the Luthier " +
            "module when building " + AGGREGATION_AVG_MAKER + " '%s'";

    @Override
    public MetricMaker build(String name, LuthierConfigNode configTable, LuthierIndustrialPark resourceFactories) {
        LuthierValidationUtils.validateField(configTable.get(INNER_GRAIN), ConceptType.METRIC_MAKER, name, INNER_GRAIN);
        String grainName = configTable.get(INNER_GRAIN).textValue();
        try {
            ZonelessTimeGrain grain = (ZonelessTimeGrain) resourceFactories.getGranularityParser()
                    .parseGranularity(grainName);
            return new AggregationAverageMaker(resourceFactories.getMetricDictionary(), grain);
        } catch (GranularityParseException | ClassCastException e) {
            throw new LuthierFactoryException(String.format(GRAIN_NAME_NOT_EXPECTED, grainName, name), e);
        }
    }
}
