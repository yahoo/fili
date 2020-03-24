// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.generator.TestRequestParameters
import com.yahoo.bard.webservice.web.util.BardConfigResources

import spock.lang.Specification

class ProtocolLogicalMetricGeneratorSpec extends Specification {

    ProtocolLogicalMetricGenerator generator

    def setup() {
        generator = new ProtocolLogicalMetricGenerator()
    }

    def "bind() throws BadApiRequestException on non-existing base LogicalMetric"() {
        // TODO
    }

    def "validate() throws BadApiRequestException on non-existing base LogicalMetric"() {
        // TODO
    }
}
