// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.mapimpl

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequest
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequestImplSpec
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException

import spock.lang.Unroll

class MetricsApiRequestMapImplSpec extends MetricsApiRequestImplSpec {

    def "check api request construction for the top level endpoint (all tables)"() {
        when:
        MetricsApiRequest apiRequest = new MetricsApiRequestMapImpl(
                Collections.emptyMap(),
                fullDictionary
        )

        then:
        apiRequest.getMetrics() as Set == fullDictionary.values() as Set
    }

    def "check api request construction for a given table name"() {
        setup:
        String name = "height"

        when:
        MetricsApiRequest apiRequest = new MetricsApiRequestMapImpl(
                Collections.singletonMap("metrics", name),
                fullDictionary
        )

        then:
        apiRequest.getMetric() == fullDictionary.get(name)
    }

    @Unroll
    def "api request construction throws #exception.simpleName because #reason"() {
        when:
        new MetricsApiRequestMapImpl(
                Collections.singletonMap("metrics", name),
                dictionary
        ).getMetrics()

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
        MetricsApiRequest apiRequest = new MetricsApiRequestMapImpl(
                Collections.emptyMap() as Map<String, String>,
                Collections.emptyMap() as Map<String, Object>,
                Collections.emptyMap() as Map<String, Object>,
                metrics as LinkedHashSet<LogicalMetric>
        )

        then:
        apiRequest.getMetrics().size() == 2
        apiRequest.getMetrics().getAt(0) == metric1
        apiRequest.getMetrics().getAt(1) == metric2
    }
}
