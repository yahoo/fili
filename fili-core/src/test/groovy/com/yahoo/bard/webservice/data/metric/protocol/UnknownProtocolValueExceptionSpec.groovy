// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol

import spock.lang.Specification

class UnknownProtocolValueExceptionSpec extends Specification {

    def "Exception forms the correct message"() {
        setup:
        String name = "name"
        Protocol protocol = Mock(Protocol)
        protocol.getContractName() >> name
        protocol.getCoreParameterName() >> name
        Map<String, String> values = [name: "bar", "otherName": "foo"]

        UnknownProtocolValueException exception = new UnknownProtocolValueException(protocol, values)

        expect:
        exception.protocol == protocol
        exception.parameterValues == values
        exception.message.endsWith("bar")
    }
}
