// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.druid.client.DruidClientConfigHelper
import com.yahoo.bard.webservice.druid.model.query.QueryContext
import com.yahoo.bard.webservice.druid.model.query.WeightEvaluationQuery

import com.fasterxml.jackson.databind.ObjectMapper

import io.netty.handler.codec.http.HttpHeaders
import spock.lang.Specification

import java.util.function.Supplier

class AsyncDruidWebServiceImplSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    def "Ensure that headersToAppend are added to request when calling postDruidQuery"() {
        setup:
        WeightEvaluationQuery weightEvaluationQuery = Mock(WeightEvaluationQuery)
        QueryContext queryContext = Mock(QueryContext)
        weightEvaluationQuery.getContext() >> queryContext

        and:
        Map<String, String> expectedHeaders = [
                k1: "v1",
                k2: "v2"
        ]
        Supplier<Map<String, String>> supplier = Mock()
        supplier.get() >> expectedHeaders

        AsyncDruidWebServiceImplWrapper webServiceImplWrapper = new AsyncDruidWebServiceImplWrapper(
                DruidClientConfigHelper.getServiceConfig(),
                MAPPER,
                supplier
        )

        when:
        webServiceImplWrapper.postDruidQuery(null, null, null, null, weightEvaluationQuery);

        then:
        HttpHeaders actualHeaders = webServiceImplWrapper.getHeaders()
        for (Map.Entry<String, String> header : expectedHeaders) {
            assert actualHeaders.get(header.getKey()) == header.getValue()
        }
    }

    def "Ensure that headersToAppend are added to request when calling getJsonObject"() {
        setup:
        Map<String, String> expectedHeaders = [
                k1: "v1",
                k2: "v2"
        ]
        Supplier<Map<String, String>> supplier = Mock()
        supplier.get() >> expectedHeaders

        AsyncDruidWebServiceImplWrapper webServiceImplWrapper = new AsyncDruidWebServiceImplWrapper(
                DruidClientConfigHelper.getServiceConfig(),
                MAPPER,
                supplier
        )

        when:
        webServiceImplWrapper.getJsonObject(null, null, null, null);

        then:
        HttpHeaders actualHeaders = webServiceImplWrapper.getHeaders()
        for (Map.Entry<String, String> header : expectedHeaders) {
            assert actualHeaders.get(header.getKey()) == header.getValue()
        }
    }
}
