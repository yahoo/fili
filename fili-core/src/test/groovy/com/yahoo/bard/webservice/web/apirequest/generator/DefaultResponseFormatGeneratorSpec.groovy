// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator

import static com.yahoo.bard.webservice.web.DefaultResponseFormatType.CSV
import static com.yahoo.bard.webservice.web.DefaultResponseFormatType.JSON
import static com.yahoo.bard.webservice.web.DefaultResponseFormatType.JSONAPI

import com.yahoo.bard.webservice.MessageFormatter
import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder
import com.yahoo.bard.webservice.web.apirequest.RequestParameters
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.util.BardConfigResources

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests for the default response format generation
 */
class DefaultResponseFormatGeneratorSpec extends Specification {

    DefaultResponseFormatGenerator generator = new DefaultResponseFormatGenerator()

    @Unroll
    def "Bind binds #formatName to #formatType"() {
        expect:
        DefaultResponseFormatGenerator.generateResponseFormat(formatName) == formatType

        where:
        formatName | formatType
        "json"     | JSON
        "JSON"     | JSON
        "jSon"     | JSON
        "jsonapi"  | JSONAPI
        "csv"      | CSV
    }

    // Testing static methods

    def "Bind binds a custom report format type"() {
        setup:
        ResponseFormatType test = Mock(ResponseFormatType)
        DefaultResponseFormatGenerator.addFormatType("test", test)

        expect:
        DefaultResponseFormatGenerator.generateResponseFormat("test") == test

        cleanup:
        DefaultResponseFormatGenerator.removeFormatType("test")
    }

    def "Bind fails on undefined format type"() {
        setup:
        String errorMessage = ErrorMessageFormat.ACCEPT_FORMAT_INVALID.format("test");

        when:
        ResponseFormatType format = DefaultResponseFormatGenerator.generateResponseFormat("test")

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage().endsWith(errorMessage)
    }

    def "Custom formatter is respected"() {
        setup:
        MessageFormatter formatter = Mock(MessageFormatter)
        formatter.format(_) >> "test"
        formatter.logFormat(_) >> "test"
        MessageFormatter old = DefaultResponseFormatGenerator.setMessageFormatter(formatter)

        when:
        ResponseFormatType format = DefaultResponseFormatGenerator.generateResponseFormat("test")

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage().endsWith("test")

        cleanup:
        DefaultResponseFormatGenerator.setMessageFormatter(old)
    }

    // Testing non-static methods
    def "Validate does nothing"() {
        setup:
        ResponseFormatType entity = Mock(ResponseFormatType)
        DataApiRequestBuilder builder = Mock(DataApiRequestBuilder)
        RequestParameters params = Mock(RequestParameters)
        BardConfigResources resources = Mock(BardConfigResources)

        when:
        generator.validate(entity, builder, params, resources)

        then:
        0 * _._
    }

    def "Bind binds a response format"() {
        setup:
        DataApiRequestBuilder builder = Mock(DataApiRequestBuilder)
        RequestParameters params = Mock(RequestParameters)
        BardConfigResources resources = Mock(BardConfigResources)

        when:
        ResponseFormatType type = generator.bind(builder, params, resources)

        then:
        type == DefaultResponseFormatType.CSV
        1 * params.getFormat() >> Optional.of("csv")
        0 * _._
    }
}
