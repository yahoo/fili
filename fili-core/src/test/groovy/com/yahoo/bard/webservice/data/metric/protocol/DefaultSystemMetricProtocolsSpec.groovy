// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol

import spock.lang.Specification

class DefaultSystemMetricProtocolsSpec extends Specification {

    ProtocolDictionary protocolDictionary = DefaultSystemMetricProtocols.DEFAULT_PROTOCOL_DICTIONARY;

    ProtocolDictionary backupDictionary;
    Set<String> backupStandardProtocols

    String testProtocol = "Foo"
    Protocol p = Mock(Protocol)

    def setup() {
        p.getContractName() >> testProtocol
        backupDictionary = new ProtocolDictionary(protocolDictionary)
        backupStandardProtocols = DefaultSystemMetricProtocols.STANDARD_PROTOCOLS
    }

    def cleanup() {
        protocolDictionary.clear()
        protocolDictionary.putAll(backupDictionary)
        backupStandardProtocols.clear()
        backupStandardProtocols.addAll(backupStandardProtocols)
    }

    def "Add a Standard Protocol modifies default contracts"() {
        when:
        DefaultSystemMetricProtocols.addAsStandardProtocol(p)

        then:
        protocolDictionary.containsKey(testProtocol)
        protocolDictionary.get(testProtocol) == p
        DefaultSystemMetricProtocols.getStandardProtocolSupport().accepts(testProtocol)
    }

    def "RemoveFromStandardProtocols"() {
        when:
        DefaultSystemMetricProtocols.addAsStandardProtocol(p)

        then:
        protocolDictionary.containsKey(testProtocol)
        protocolDictionary.get(testProtocol) == p
        DefaultSystemMetricProtocols.getStandardProtocolSupport().accepts(testProtocol)

        when:
        DefaultSystemMetricProtocols.removeFromStandardProtocols(testProtocol)

        then:
        ! DefaultSystemMetricProtocols.getStandardProtocolSupport().accepts(testProtocol)

    }

    def "GetStandardProtocolSupport wraps new protocols"() {
        setup:
        DefaultSystemMetricProtocols.addAsStandardProtocol(p)
        ProtocolSupport protocolSupport = DefaultSystemMetricProtocols.getStandardProtocolSupport()

        expect:
        protocolSupport.accepts(testProtocol)
        protocolSupport.getProtocol(testProtocol) == p
    }
}
