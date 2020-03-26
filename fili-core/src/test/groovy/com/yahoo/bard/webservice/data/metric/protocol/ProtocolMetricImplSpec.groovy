// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper

import spock.lang.Specification

class ProtocolMetricImplSpec extends Specification {

    ProtocolSupport protocolSupport = Mock(ProtocolSupport)
    ProtocolMetricImpl protocolMetric;
    Protocol protocol = Mock(Protocol)

    String protocolName = "foo"

    def setup() {
        ResultSetMapper resultSetMapper = new NoOpResultSetMapper()
        GeneratedMetricInfo logicalMetricInfo = new GeneratedMetricInfo("name", "baseName")
        TemplateDruidQuery templateDruidQuery = Mock(TemplateDruidQuery)
        protocolMetric = new ProtocolMetricImpl(logicalMetricInfo, templateDruidQuery, resultSetMapper, protocolSupport)
    }

    def "Accept invokes the protocol support and applies the attached transformer"() {
        setup:
        LogicalMetric expected = Mock(LogicalMetric)
        GeneratedMetricInfo outLmi = Mock(GeneratedMetricInfo)
        MetricTransformer metricTransformer = Mock(MetricTransformer)
        protocolSupport.getProtocol(protocolName) >> protocol
        protocol.getMetricTransformer() >> metricTransformer

        Map values = [:]

        when:
        LogicalMetric result = protocolMetric.accept(outLmi, protocolName, values)

        then:
        1* metricTransformer.apply(outLmi, protocolMetric, protocol, values) >> expected
        result == expected
    }

    def "Accept throws an exception with a bad values"() {
        setup:
        LogicalMetric expected = Mock(LogicalMetric)
        GeneratedMetricInfo outLmi = Mock(GeneratedMetricInfo)
        MetricTransformer metricTransformer = Mock(MetricTransformer)
        protocolSupport.getProtocol(protocolName) >> protocol
        protocol.getCoreParameterName() >> protocolName
        protocol.getMetricTransformer() >> metricTransformer

        Map values = ["foo": "bar"]

        when:
        LogicalMetric result = protocolMetric.accept(outLmi, protocolName, values)

        then:
        UnknownProtocolValueException exception = thrown(UnknownProtocolValueException)
        1 * metricTransformer.apply(outLmi, protocolMetric, protocol, values) >> {
            throw new UnknownProtocolValueException(protocol, values)
        }
        exception.getParameterValues() == values
        exception.getMessage().contains(protocolName)
    }
}
