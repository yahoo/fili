// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl

import com.yahoo.bard.webservice.druid.client.DruidClientConfigHelper
import com.yahoo.bard.webservice.druid.model.query.QueryContext
import com.yahoo.bard.webservice.druid.model.query.WeightEvaluationQuery

import com.fasterxml.jackson.databind.ObjectMapper

import io.netty.handler.codec.http.HttpHeaders
import spock.lang.Specification

import java.util.function.Supplier

class AsyncDruidWebServiceImplSpec extends Specification {
    def "Ensure that headersToAppend are added"() {
        setup:
        WeightEvaluationQuery weightEvaluationQuery = Mock(WeightEvaluationQuery)
        QueryContext queryContext = Mock(QueryContext)
        weightEvaluationQuery.getContext() >> { queryContext }
        queryContext.numberOfQueries() >> { 1 }
        queryContext.getSequenceNumber() >> { 1 }

        and:
        Map<String, String> expectedHeaders = new HashMap<>()
        expectedHeaders.put("k1", "v1")
        expectedHeaders.put("k2", "v2")
        Supplier<Map<String, String>> supplier = new Supplier<Map<String, String>>() {
            @Override
            Map<String, String> get() {
                return expectedHeaders
            }
        }
        AsyncDruidWebServiceImplWrapper webServiceImplWrapper = new AsyncDruidWebServiceImplWrapper(
                DruidClientConfigHelper.getNonUiServiceConfig(),
                new ObjectMapper(),
                supplier
        )

        when:
        webServiceImplWrapper.postDruidQuery(
                null,
                null,
                null,
                null,
                weightEvaluationQuery
        );

        then:
        HttpHeaders actualHeaders = webServiceImplWrapper.getHeaders()
        for (Map.Entry<String, String> header : expectedHeaders) {
            assert actualHeaders.get(header.getKey()) == header.getValue()
        }
    }
}
