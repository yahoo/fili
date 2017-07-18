// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.config.metric.makers

import com.yahoo.fili.webservice.data.metric.LogicalMetric
import com.yahoo.fili.webservice.data.metric.MetricDictionary
import com.yahoo.fili.webservice.data.metric.mappers.RowNumMapper

import spock.lang.Specification


class RowNumMakerSpec extends Specification {

    private static final String METRIC_NAME = "Row Num Generator"
    private static final String DESCRIPTION = "Generator for Row Numbers"

    def "Build a logical metric that generates row numbers"() {
        given: "A logical metric that generates row numbers"
        LogicalMetric metric = new LogicalMetric(null, new RowNumMapper(), METRIC_NAME, DESCRIPTION)

        expect:
        //RowSumMaker does not rely on the metric dictionary.
        new RowNumMaker(new MetricDictionary()).make(METRIC_NAME, []) == metric
    }
}
