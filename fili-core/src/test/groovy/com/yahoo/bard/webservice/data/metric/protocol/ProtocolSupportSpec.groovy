// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol


import spock.lang.Specification

class ProtocolSupportSpec extends Specification {

    MetricTransformer metricTransformer = Mock(MetricTransformer)

    Protocol fooProtocol
    Protocol barProtocol
    Protocol bazProtocol

    ProtocolSupport withFooWithoutBarProtocolSupport, withAndWithoutBarProtocolSupport

    String protocolName1 = "foo"
    String protocolName2 = "bar"
    String protocolName3 = "baz"


    def setup() {
        fooProtocol = new Protocol(protocolName1, metricTransformer)
        barProtocol = new Protocol(protocolName2, metricTransformer)
        bazProtocol = new Protocol(protocolName3, metricTransformer)
        withFooWithoutBarProtocolSupport = new ProtocolSupport([fooProtocol], [protocolName2] as HashSet)
        withAndWithoutBarProtocolSupport = new ProtocolSupport([fooProtocol], [protocolName1] as HashSet)
    }

    def "Accepts is true configured values that are configured and not blacklisted"() {
        expect:
        withFooWithoutBarProtocolSupport.accepts("foo")
        ! withFooWithoutBarProtocolSupport.accepts("bar")
        ! withFooWithoutBarProtocolSupport.accepts("baz")
        ! withAndWithoutBarProtocolSupport.accepts("foo")
        ! withAndWithoutBarProtocolSupport.accepts("bar")
        ! withAndWithoutBarProtocolSupport.accepts("baz")
    }

    def "isBlacklisted is true for blacklisted fields"() {
        expect:
        ! withFooWithoutBarProtocolSupport.isBlacklisted("foo")
        withFooWithoutBarProtocolSupport.isBlacklisted("bar")
        ! withFooWithoutBarProtocolSupport.isBlacklisted("baz")
        withAndWithoutBarProtocolSupport.isBlacklisted("foo")
        ! withAndWithoutBarProtocolSupport.isBlacklisted("bar")
        ! withAndWithoutBarProtocolSupport.isBlacklisted("baz")
    }


    def "BlacklistProtocol supresses both previously unknown and known protocols"() {
        setup:
        ProtocolSupport test1 = withFooWithoutBarProtocolSupport.blacklistProtocol("foo")
        ProtocolSupport test2 = withAndWithoutBarProtocolSupport.blacklistProtocol("baz")

        expect:
        test1.isBlacklisted("foo")
        ! test1.accepts("foo")

        test1.isBlacklisted("bar")
        ! test1.accepts("bar")

        ! test1.isBlacklisted("baz")
        ! test1.accepts("baz")

        test2.isBlacklisted("foo")
        ! test2.accepts("foo")

        ! test2.isBlacklisted("bar")
        ! test2.accepts("bar")

        test2.isBlacklisted("baz")
        ! test2.accepts("baz")
    }

    def "Without protocol support supresses both previously unknown and known protocols"() {
        setup:
        ProtocolSupport subtractFooAndBaz = new ProtocolSupport([barProtocol], ["foo", "baz"] as LinkedHashSet)
        ProtocolSupport noToAll = withFooWithoutBarProtocolSupport.mergeBlacklists([subtractFooAndBaz])

        ProtocolSupport subtractBaz = new ProtocolSupport([barProtocol], ["baz"] as LinkedHashSet)
        ProtocolSupport fooNoBarBaz = withFooWithoutBarProtocolSupport.mergeBlacklists([subtractBaz])

        expect:
        noToAll.isBlacklisted("foo")
        noToAll.isBlacklisted("bar")
        noToAll.isBlacklisted("baz")

        fooNoBarBaz.accepts("foo")
        fooNoBarBaz.isBlacklisted("bar")
        fooNoBarBaz.isBlacklisted("baz")
    }


    def "Without protocols supresses both previously unknown and known protocols"() {
        setup:
        ProtocolSupport test1 = withFooWithoutBarProtocolSupport.blackListProtocols(["foo", "baz"])

        expect:
        test1.isBlacklisted("foo")
        test1.isBlacklisted("bar")
        test1.isBlacklisted("baz")
    }

    def "With protocols approves both previously unknown and known protocols"() {
        setup:
        ProtocolSupport test1 = withFooWithoutBarProtocolSupport.withProtocols([fooProtocol, barProtocol, bazProtocol])

        expect:
        test1.accepts("foo")
        test1.accepts("bar")
        test1.accepts("baz")
    }

    def "Combine blacklists combines blacklists"() {
        setup:
        ProtocolSupport protocolSupport2 = new ProtocolSupport([fooProtocol], [protocolName1] as LinkedHashSet)
        ProtocolSupport test = withFooWithoutBarProtocolSupport.mergeBlacklists([protocolSupport2] as LinkedHashSet)

        expect:
        test.isBlacklisted(protocolName1)
        test.isBlacklisted(protocolName2)
        ! test.accepts(protocolName3)
        ! test.isBlacklisted(protocolName3)

    }
}
