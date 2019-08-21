// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders

import com.yahoo.bard.webservice.web.BadApiRequestException
import com.yahoo.bard.webservice.web.DefaultResponseFormatType

import spock.lang.Specification

class DefaultResponseFormatTypeGeneratorSpec extends Specification {

    def "check valid parsing generateFormat"() {

        expect:
        responseFormat == expectedFormat

        where:
        responseFormat                 | expectedFormat
        DefaultResponseFormatType.JSON | ResponseFormatTypeGenerator.DEFAULT_RESPONSE_FORMAT_TYPE_GENERATOR.generateAcceptFormat(null)
        DefaultResponseFormatType.JSON | ResponseFormatTypeGenerator.DEFAULT_RESPONSE_FORMAT_TYPE_GENERATOR.generateAcceptFormat("json")
        DefaultResponseFormatType.CSV  | ResponseFormatTypeGenerator.DEFAULT_RESPONSE_FORMAT_TYPE_GENERATOR.generateAcceptFormat("csv")
    }

    def "check invalid parsing generateFormat"() {
        when:
        ResponseFormatTypeGenerator.DEFAULT_RESPONSE_FORMAT_TYPE_GENERATOR.generateAcceptFormat("bad")

        then:
        thrown BadApiRequestException
    }
}
