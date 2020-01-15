// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories

import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark
import com.yahoo.bard.webservice.data.metric.LogicalMetric

import spock.lang.Specification

class DefaultMetricFactorySpec extends Specification {
    LuthierIndustrialPark park = new LuthierIndustrialPark.Builder().build()
    LogicalMetric logicalMetric

    void setup() {
        park.load()
    }

    def "a particular longSum metric correctly builds from the LIP"() {
        when:
            logicalMetric = park.getMetric("longSumCO")
        then:
            logicalMetric.longName == "longSumCO"
            logicalMetric.name == "longSumCO"
            logicalMetric.description == "longSumCO"
            logicalMetric.category == "GENERAL"
            logicalMetric.type == "number"
    }
}
