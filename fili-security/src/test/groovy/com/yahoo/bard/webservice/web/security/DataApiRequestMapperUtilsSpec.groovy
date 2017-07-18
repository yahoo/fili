// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.web.ApiRequest
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.RequestValidationException

import spock.lang.Specification

import java.util.function.BiFunction

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.Response

class DataApiRequestMapperUtilsSpec extends Specification {

    ResourceDictionaries dictionaries = Mock(ResourceDictionaries)
    ApiRequest request = Mock(ApiRequest)
    ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)

    def "Identity only returns the identity"() {
        given: "A simple identity mapper"
        RequestMapper mapper = DataApiRequestMapperUtils.identityMapper(dictionaries)

        and: "No interactions with mocks occur"
        0 * _

        expect:
        mapper.apply(request, containerRequestContext) == request
    }

    def "Validation exception mapper returns a built exception"() {
        given: "A mapper expecting an exception"
        RequestValidationException expected = new RequestValidationException(Response.Status.OK, "Test", "Test")

        BiFunction<ApiRequest, ContainerRequestContext, RequestValidationException> exceptionBuilder = new
                BiFunction<ApiRequest, ContainerRequestContext, RequestValidationException>() {
                    @Override
                    RequestValidationException apply(
                            final ApiRequest apiRequest,
                            final ContainerRequestContext containerRequestContext
                    ) {
                        return expected
                    }
                }

        RequestMapper mapper = DataApiRequestMapperUtils.validationExceptionMapper(dictionaries, exceptionBuilder)
        exceptionBuilder.apply(containerRequestContext) >> expected

        when:
        mapper.apply(request, containerRequestContext)

        then:
        true
        Exception actual = thrown(RequestValidationException)
        actual == expected
    }
}
