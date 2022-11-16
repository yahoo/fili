// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol

import static java.util.Collections.singleton

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification

class ProtocolMetricImplSpec extends Specification {

    ProtocolSupport protocolSupport = Mock(ProtocolSupport)
    ProtocolMetricImpl protocolMetric
    Protocol protocol = Mock(Protocol)

    String protocolName = "foo"

    def setup() {
        ResultSetMapper resultSetMapper = new NoOpResultSetMapper()
        GeneratedMetricInfo logicalMetricInfo = new GeneratedMetricInfo("name", "baseName")
        TemplateDruidQuery templateDruidQuery = Mock(TemplateDruidQuery)
        protocolMetric = new ProtocolMetricImpl(
                logicalMetricInfo,
                templateDruidQuery,
                resultSetMapper,
                protocolSupport,
                []
        )
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

    def "ExtendedMetricDependencies use default values"() {
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
        GeneratedMetricInfo outLmi = Mock(GeneratedMetricInfo)
        MetricTransformer metricTransformer = Mock(MetricTransformer)
        protocolSupport.getProtocol(protocolName) >> protocol
        protocol.getCoreParameterName() >> protocolName
        protocol.getMetricTransformer() >> metricTransformer

        Map values = ["foo": "bar"]

        when:
        protocolMetric.accept(outLmi, protocolName, values)

        then:
        UnknownProtocolValueException exception = thrown(UnknownProtocolValueException)
        1 * metricTransformer.apply(outLmi, protocolMetric, protocol, values) >> {
            throw new UnknownProtocolValueException(protocol, values)
        }
        exception.getParameterValues() == values
        exception.getMessage().contains(protocolName)
    }

    def "Transitive dependencies get added to metric dependencies"() {
        setup:
        Interval nearNow = new Interval("2022-02-22/P1D")
        Interval longAgoIsh = new Interval("1975-11-27/P1D")
        Interval now = new Interval("2022-03-02/P1D")

        ProtocolMetricImpl transitive1 = Mock(ProtocolMetricImpl)
        transitive1.getDependentInterval(_ as SimplifiedIntervalList, _ as Granularity) >> new SimplifiedIntervalList(
                singleton(nearNow))

        ProtocolMetricImpl transitive2 = Mock(ProtocolMetricImpl)
        transitive2.getDependentInterval(_ as SimplifiedIntervalList, _ as Granularity) >> new SimplifiedIntervalList(
                singleton(longAgoIsh))

        TemplateDruidQuery templateDruidQuery = Mock(TemplateDruidQuery)

        ProtocolMetric m = new ProtocolMetricImpl(
                new LogicalMetricInfo("foo"),
                templateDruidQuery,
                new NoOpResultSetMapper(),
                [transitive1, transitive2]
        )

        SimplifiedIntervalList nowList = m.getDependentInterval(new SimplifiedIntervalList(singleton(now)), DefaultTimeGrain.DAY)

        expect:
        nowList.contains(now)
        nowList.contains(nearNow)
        nowList.contains(longAgoIsh)
        ! nowList.contains(new SimplifiedIntervalList(singleton(new Interval("2022-04-01/P1D"))))
    }
}
