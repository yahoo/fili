// Copyright 2021 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.beanimpl


import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequestImplSpec
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException

import spock.lang.Unroll

class MetricsApiRequestBeanImplSpec extends MetricsApiRequestImplSpec {

    def "check api request construction for the top level endpoint (all tables)"() {
        when:
        MetricsApiRequestBeanImpl apiRequest = new MetricsApiRequestBeanImpl(
                null,  // metricName
                null,  // format
                "", //fileName
                "",  // perPage
                "",  // page
                fullDictionary
        )

        then:
        apiRequest.getMetrics() as Set == fullDictionary.values() as Set
    }

    def "check api request construction for a given table name"() {
        setup:
        String name = "height"

        when:
        MetricsApiRequestBeanImpl apiRequest = new MetricsApiRequestBeanImpl(
                name,
                null,  // format
                "",
                "",  // perPage
                "",  // page
                fullDictionary
        )

        then:
        apiRequest.getMetric() == fullDictionary.get(name)
    }

    @Unroll
    def "api request construction throws #exception.simpleName because #reason"() {
        when:
        new MetricsApiRequestBeanImpl(
                name,
                null,  // format
                "",
                "",  // perPage
                "",  // page
                dictionary
        )

        then:
        Exception e = thrown(exception)
        e.getMessage().matches(reason)

        where:
        name     | dictionary      | exception              | reason
        "height" | emptyDictionary | BadApiRequestException | ".*Metric.*do not exist.*"
        "weight" | fullDictionary  | BadApiRequestException | ".*Metric.*do not exist.*"
    }

    def "re-write api request with desired metrics"() {
        when:
        MetricsApiRequestBeanImpl apiRequest = new MetricsApiRequestBeanImpl(
                null,
                null,
                null,
                metrics
        )

        then:
        apiRequest.getMetrics().size() == 2
        apiRequest.getMetrics().getAt(0) == metric1
        apiRequest.getMetrics().getAt(1) == metric2
    }
}
