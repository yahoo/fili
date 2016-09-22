// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import spock.lang.Specification

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap

class DataApiRequestTypeIdentifierSpec extends Specification {

    def "check if UI request is detected"() {
        setup:
        MultivaluedMap header = new MultivaluedHashMap()
        header.putSingle("clientid", "UI")
        header.putSingle("referer", ".")

        expect:
        DataApiRequestTypeIdentifier.isUi(header) == true
    }

    def "check if test request is detected"() {
        setup:
        MultivaluedMap header = new MultivaluedHashMap()
        header.putSingle("bard-testing", "###BYPASS###")

        expect:
        DataApiRequestTypeIdentifier.isBypass(header) == true
    }

    def "check if CORS pre-flight request is detected"() {

        expect:
        DataApiRequestTypeIdentifier.isCorsPreflight("OPTIONS", null) == true
    }
}
