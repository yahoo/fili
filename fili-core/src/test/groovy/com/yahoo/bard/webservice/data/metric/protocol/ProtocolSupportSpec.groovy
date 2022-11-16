// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol


import spock.lang.Specification

class ProtocolSupportSpec extends Specification {

    MetricTransformer metricTransformer = Mock(MetricTransformer)

    Protocol fooProtocol
    Protocol barProtocol
    Protocol bazProtocol
    Protocol notFooButParameterFoo

    ProtocolSupport withFooBlacklistBar, withFooBlacklistFoo
    ProtocolSupport withNotFooAndBar, withNotFooAndBarBlacklistFoo

    String protocolName1 = "foo"
    String protocolName2 = "bar"
    String protocolName3 = "baz"
    String protocolNameNotFoo = "notFoo"

    def setup() {
        fooProtocol = new Protocol(protocolName1, metricTransformer)
        barProtocol = new Protocol(protocolName2, metricTransformer)
        bazProtocol = new Protocol(protocolName3, metricTransformer)
        notFooButParameterFoo = new Protocol(protocolNameNotFoo, protocolName1, metricTransformer)

        withFooBlacklistBar = new ProtocolSupport([fooProtocol], [protocolName2] as HashSet)
        withFooBlacklistFoo = new ProtocolSupport([fooProtocol], [protocolName1] as HashSet)
        withNotFooAndBar = new ProtocolSupport([notFooButParameterFoo, barProtocol])
        withNotFooAndBarBlacklistFoo = new ProtocolSupport([notFooButParameterFoo, barProtocol], [protocolName1] as HashSet)
    }

    def "Accepts is true configured values that are configured and not blacklisted"() {
        expect:
        withFooBlacklistBar.accepts("foo")
        withNotFooAndBarBlacklistFoo.accepts(protocolNameNotFoo)
        ! withFooBlacklistBar.accepts("bar")
        ! withFooBlacklistBar.accepts("baz")
        ! withFooBlacklistFoo.accepts("foo")
        ! withFooBlacklistFoo.accepts("bar")
        ! withFooBlacklistFoo.accepts("baz")
    }

    def "isBlacklisted is true for blacklisted fields"() {
        expect:
        ! withFooBlacklistBar.isBlacklisted("foo")
        withFooBlacklistBar.isBlacklisted("bar")
        ! withFooBlacklistBar.isBlacklisted("baz")
        withFooBlacklistFoo.isBlacklisted("foo")
        ! withFooBlacklistFoo.isBlacklisted("bar")
        ! withFooBlacklistFoo.isBlacklisted("baz")
    }

    def "BlacklistProtocol supresses both previously unknown and known protocols"() {
        setup:
        ProtocolSupport test1 = withFooBlacklistBar.blacklistProtocol("foo")
        ProtocolSupport test2 = withFooBlacklistFoo.blacklistProtocol("baz")

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
        ProtocolSupport noToAll = withFooBlacklistBar.mergeBlacklists([subtractFooAndBaz])

        ProtocolSupport subtractBaz = new ProtocolSupport([barProtocol], ["baz"] as LinkedHashSet)
        ProtocolSupport fooNoBarBaz = withFooBlacklistBar.mergeBlacklists([subtractBaz])

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
        ProtocolSupport test1 = withFooBlacklistBar.blackListProtocols(["foo", "baz"])

        expect:
        test1.isBlacklisted("foo")
        test1.isBlacklisted("bar")
        test1.isBlacklisted("baz")
    }

    def "With protocols approves both previously unknown and known protocols"() {
        setup:
        ProtocolSupport test1 = withFooBlacklistBar.withProtocols([fooProtocol, barProtocol, bazProtocol])

        expect:
        test1.accepts("foo")
        test1.accepts("bar")
        test1.accepts("baz")
    }

    def "With protocols throws an error if adding a protocol whose parameter is already in use"() {
        when:
        ProtocolSupport test1 = withFooBlacklistBar.withProtocols([notFooButParameterFoo])

        then:
        thrown(IllegalStateException)
    }


    def "With replace protocols replaces successfully a protocol whose parameter is already in use"() {
        when:
        ProtocolSupport test1 = withFooBlacklistBar.withReplaceProtocols([notFooButParameterFoo])

        then:
        test1.getProtocol(protocolName1) == null
        test1.getProtocol(notFooButParameterFoo.getContractName()) == notFooButParameterFoo
        test1.acceptsParameter(protocolName1)
    }

    def "Combine blacklists combines blacklists"() {
        setup:
        ProtocolSupport protocolSupport2 = new ProtocolSupport([fooProtocol], [protocolName1] as LinkedHashSet)
        ProtocolSupport test = withFooBlacklistBar.mergeBlacklists([protocolSupport2] as LinkedHashSet)

        expect:
        test.isBlacklisted(protocolName1)
        test.isBlacklisted(protocolName2)
        ! test.accepts(protocolName3)
        ! test.isBlacklisted(protocolName3)

    }

    def "Error when two protocols collide on parameter name"() {
        when:
        ProtocolSupport protocolSupport = new ProtocolSupport([fooProtocol, notFooButParameterFoo])

        then:
        thrown(IllegalStateException)
    }

    def "Accepts parameter "() {
        expect:
        withFooBlacklistBar.acceptsParameter("foo")
        ! withFooBlacklistBar.acceptsParameter("bar")
        ! withFooBlacklistBar.acceptsParameter("baz")
        ! withFooBlacklistFoo.acceptsParameter("foo")
        ! withFooBlacklistFoo.acceptsParameter("bar")
        ! withFooBlacklistFoo.acceptsParameter("baz")

        and:
        withNotFooAndBar.acceptsParameter("foo") && ! withNotFooAndBar.accepts("foo")
        withNotFooAndBarBlacklistFoo.acceptsParameter("foo") && ! withNotFooAndBarBlacklistFoo.accepts("foo")
    }
}
