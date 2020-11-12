// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol

import spock.lang.Specification

class DefaultSystemMetricProtocolsSpec extends Specification {

    static final ProtocolDictionary BACKUP_DICTIONARY = new ProtocolDictionary(DefaultSystemMetricProtocols.DEFAULT_PROTOCOL_DICTIONARY);
    static final Set<String> BACKUP_STANDARD_PROTOCOLS = new LinkedHashSet<>(DefaultSystemMetricProtocols.STANDARD_PROTOCOLS)

    ProtocolDictionary protocolDictionary

    String testProtocol = "Foo"
    Protocol p = Mock(Protocol)

    def setup() {
        p.getContractName() >> testProtocol
        protocolDictionary = DefaultSystemMetricProtocols.DEFAULT_PROTOCOL_DICTIONARY
    }

    def cleanup() {
        DefaultSystemMetricProtocols.DEFAULT_PROTOCOL_DICTIONARY.clear()
        DefaultSystemMetricProtocols.DEFAULT_PROTOCOL_DICTIONARY.putAll(BACKUP_DICTIONARY)
        DefaultSystemMetricProtocols.STANDARD_PROTOCOLS.clear()
        DefaultSystemMetricProtocols.STANDARD_PROTOCOLS.addAll(BACKUP_STANDARD_PROTOCOLS)
    }

    def setupSpec() {
    }

    def cleanupSpec() {
        DefaultSystemMetricProtocols.DEFAULT_PROTOCOL_DICTIONARY.clear()
        DefaultSystemMetricProtocols.DEFAULT_PROTOCOL_DICTIONARY.putAll(BACKUP_DICTIONARY)
        DefaultSystemMetricProtocols.STANDARD_PROTOCOLS.clear()
        DefaultSystemMetricProtocols.STANDARD_PROTOCOLS.addAll(BACKUP_STANDARD_PROTOCOLS)
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
