// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.endpoints.MetricsServlet

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

abstract class MetricsApiRequestImplSpec extends Specification {

    @Shared
    MetricDictionary fullDictionary

    @Shared
    MetricDictionary emptyDictionary = new MetricDictionary()

    @Shared
    LinkedHashSet<LogicalMetric> metrics = new LinkedHashSet<>()

    LogicalMetric metric1
    LogicalMetric metric2

    def setupSpec() {
        JerseyTestBinder jtb = new JerseyTestBinder(MetricsServlet.class)
        fullDictionary = jtb.configurationLoader.metricDictionary
        jtb.tearDown()
    }

    def setup() {
        metric1 = new LogicalMetricImpl(null, null, "met1")
        metric2 = new LogicalMetricImpl(null, null, "met2")
        metrics.add(metric1)
        metrics.add(metric2)
    }
}
