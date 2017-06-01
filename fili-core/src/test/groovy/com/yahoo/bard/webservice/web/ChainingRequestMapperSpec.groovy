// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.data.config.ResourceDictionaries

import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext

class ChainingRequestMapperSpec extends Specification {

    RequestMapper nextMapper = Mock(RequestMapper)
    ApiRequest mockRequest = Mock(ApiRequest)
    ResourceDictionaries resourceDictionaries = new ResourceDictionaries()
    ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)

    def "Test that the chaining request processor delegates to the next mapper"() {
        setup:
        1 * mockRequest.clone() >> mockRequest
        1 * nextMapper.apply(mockRequest, containerRequestContext) >> mockRequest

        ChainingRequestMapper instance = new ChainingRequestMapper(resourceDictionaries, nextMapper) {
            ApiRequest internalApply(final ApiRequest request, final ContainerRequestContext context)
                    throws RequestValidationException {
                return mockRequest.clone()
            }
        }

        expect:
        instance.apply(mockRequest, containerRequestContext) == mockRequest
    }
}
