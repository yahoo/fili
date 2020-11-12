// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric.protocol

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo
import com.yahoo.bard.webservice.data.metric.protocol.MetadataApplyTransformer
import com.yahoo.bard.webservice.data.metric.protocol.MetricTransformer
import com.yahoo.bard.webservice.data.metric.protocol.Protocol
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetric
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolSupport
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric

import spock.lang.Ignore
import spock.lang.Specification

class ProtocolChainSpec extends Specification {

    String protocol1Name
    MetricTransformer transformer1
    Protocol p1

    String protocol2Name
    MetricTransformer transformer2
    Protocol p2

    String protocol3Name
    Protocol p3

    String interactionMetricRawName
    String interactionMetricBaseName
    GeneratedMetricInfo interactionMetricResultMetadata
    ProtocolMetric interactionMetric

    ProtocolSupport protocolSupport = Mock(ProtocolSupport)

    def setup() {
        protocol1Name = "p1"
        transformer1 = new MetadataApplyTransformer()
        p1 = new Protocol(protocol1Name, transformer1)

        protocol2Name = "p2"
        transformer2 = new MetadataApplyTransformer()
        p2 = new Protocol(protocol2Name, transformer2)

        protocol3Name = "p3"
        p3 = new Protocol(protocol3Name, protocol1Name, transformer2)


        interactionMetricRawName = "RAW_interactionMetric"
        interactionMetricBaseName = "interactionMetric"
        interactionMetricResultMetadata = new GeneratedMetricInfo(interactionMetricRawName, interactionMetricBaseName)
        interactionMetric = Mock(ProtocolMetric)
        interactionMetric.getProtocolSupport() >> protocolSupport
    }

    def "Accepted core parameter is applied to protocol metric"() {
        setup:
        ProtocolChain chain = new ProtocolChain([p1], true)
        Map<String, String> parameters = [(protocol1Name): "value"]
        ApiMetric testApiMetric = new ApiMetric(
                interactionMetricRawName,
                interactionMetricBaseName,
                parameters,
        )

        when:
        chain.applyProtocols(interactionMetricResultMetadata, testApiMetric, interactionMetric)

        then:
        1 * protocolSupport.acceptsParameter(protocol1Name) >> true
        1 * protocolSupport.accepts(protocol1Name) >> true
        1 * interactionMetric.accept(interactionMetricResultMetadata, protocol1Name, parameters)
        1 * interactionMetric.getProtocolSupport() >> protocolSupport
        0 * protocolSupport.accepts(_)
        0 * interactionMetric._
    }

    def "Unrecognized core parameters are ignored"() {
        setup:
        ProtocolChain chain = new ProtocolChain([p1], true)

        Map<String, String> parameters = [
                (protocol1Name): "unused",
                unrecognizedProtocol: "unused"
        ]
        ApiMetric testApiMetric = new ApiMetric(
                interactionMetricRawName,
                interactionMetricBaseName,
                parameters,
        )

        when:
        chain.applyProtocols(interactionMetricResultMetadata, testApiMetric, interactionMetric)

        then: "only the recognized parameter is checked and applied"
        1 * interactionMetric.getProtocolSupport() >> protocolSupport
        1 * protocolSupport.accepts(protocol1Name) >> true
        1 * protocolSupport.acceptsParameter(protocol1Name) >> true
        1 * interactionMetric.accept(interactionMetricResultMetadata, protocol1Name, parameters) // all provided parameters are passed to accept
        0 * interactionMetric._
        0 * protocolSupport._
    }

    def "Protocol metrics that produce non-protocol metrics successfully finish apply loop with no errors"() {
        setup: "add 2 metrics to chain, but only p1 should ever be applied in this test"
        ProtocolChain chain = new ProtocolChain([p1, p2], true)

         Map<String, String> parameters = [
                 (protocol1Name): "unused",
                 (protocol2Name): "unused",
         ]
        ApiMetric testApiMetric = new ApiMetric(
                interactionMetricRawName,
                interactionMetricBaseName,
                parameters
        )
        LogicalMetric resultInteractionMetric = Mock(LogicalMetric)

        when:
        LogicalMetric expected = chain.applyProtocols(interactionMetricResultMetadata, testApiMetric, interactionMetric)

        then: "p1 is successfully applied and produces a standard logical metric"
        1 * interactionMetric.getProtocolSupport() >> protocolSupport
        1 * protocolSupport.accepts(protocol1Name) >> true
        1 * protocolSupport.acceptsParameter(protocol1Name) >> true
        1 * interactionMetric.accept(interactionMetricResultMetadata, protocol1Name, parameters) >> resultInteractionMetric

        and: "no more protocols are attempted to be applied to the original metric"
        0 * interactionMetric._
        0 * protocolSupport._

        and: "no protocols are attempted to be applied to the new metric"
        0 * resultInteractionMetric._

        and: "finally, the new metric is successfully returned without issue"
        expected == resultInteractionMetric
    }

    def "Strict validation fails the query if attempted to apply unrecognized protocol"() {
        setup:
        ProtocolChain chain = new ProtocolChain([p1], true)
        Map<String, String> parameters = [(protocol1Name): "unused"]
        ApiMetric testApiMetric = new ApiMetric(
                interactionMetricRawName,
                interactionMetricBaseName,
                parameters
        )

        when:
        chain.applyProtocols(interactionMetricResultMetadata, testApiMetric, interactionMetric)

        then:
        1 * interactionMetric.getProtocolSupport() >> protocolSupport
        1 * protocolSupport.acceptsParameter(protocol1Name) >> false
        0 * protocolSupport._
        thrown(IllegalArgumentException)
    }

    // TODO rewrite this test. Metric should NOT be transformed, BUT is renamed! even noop rename
    def "Non-strict validation ignores protocols that are recognized but not accepted by the target metric"() {
        setup:
        ProtocolChain chain = new ProtocolChain([p1]) // not strict validation for this test
        Map<String, String> parameters = [(protocol1Name): "unused"]
        ApiMetric testApiMetric = new ApiMetric(
                interactionMetricRawName,
                interactionMetricBaseName,
                parameters,
        )

        when:
        LogicalMetric expected = chain.applyProtocols(interactionMetricResultMetadata, testApiMetric, interactionMetric)

        then:
        1 * interactionMetric.getProtocolSupport() >> protocolSupport
        1 * protocolSupport.acceptsParameter(protocol1Name) >> false
        0 * interactionMetric._
        0 * protocolSupport._
        expected == interactionMetric
    }

    def "When two protocols have the same parameters and the match is on the first protocol the second protocol isn't triggered"() {
        setup:
        ProtocolChain chain = new ProtocolChain([p1, p2, p3], true)
        Map<String, String> parameters = [(protocol1Name): "value"]
        ApiMetric testApiMetric = new ApiMetric(
                interactionMetricRawName,
                interactionMetricBaseName,
                parameters,
        )

        when:
        chain.applyProtocols(interactionMetricResultMetadata, testApiMetric, interactionMetric)

        then:
        1 * protocolSupport.acceptsParameter(protocol1Name) >> true
        1 * protocolSupport.accepts(protocol1Name) >> true
        1 * interactionMetric.accept(interactionMetricResultMetadata, protocol1Name, parameters) >> interactionMetric
        // Because the parameter is removed of the first pass, this only fires once
        1 * interactionMetric.getProtocolSupport() >> protocolSupport
        0 * protocolSupport.accepts(_)
        0 * interactionMetric._
    }

    def "When two protocols have the same parameters and the match is on the second protocol the first is skipped"() {
        setup:
        ProtocolChain chain = new ProtocolChain([p3, p1, p2], true)
        Map<String, String> parameters = [(protocol1Name): "value"]
        ApiMetric testApiMetric = new ApiMetric(
                interactionMetricRawName,
                interactionMetricBaseName,
                parameters,
        )

        when:
        chain.applyProtocols(interactionMetricResultMetadata, testApiMetric, interactionMetric)

        then:
        2 * protocolSupport.acceptsParameter(protocol1Name) >> true
        1 * protocolSupport.accepts(protocol3Name) >> false
        1 * protocolSupport.accepts(protocol1Name) >> true
        1 * interactionMetric.accept(interactionMetricResultMetadata, protocol1Name, parameters) >> interactionMetric
        2 * interactionMetric.getProtocolSupport() >> protocolSupport
        0 * protocolSupport.accepts(_)
        0 * interactionMetric._
    }
}
