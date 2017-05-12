// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl

import com.yahoo.bard.webservice.druid.client.DruidServiceConfig
import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

import org.asynchttpclient.Response

import java.nio.charset.StandardCharsets
import java.util.function.Supplier

class AsyncDruidWebServiceImplV2Spec extends Specification {
    def "Make sure X-Druid-Response-Context and status-code are merged into existing JsonNode"() {
        given:
        Response response = Mock(Response)
        response.getResponseBodyAsStream() >> new ByteArrayInputStream('[{"k1":"v1"}]'.getBytes(StandardCharsets.UTF_8))
        response.getHeader("X-Druid-Response-Context") >>
                '''
                    {
                        "uncoveredIntervals": [
                            "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                            "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                        ],
                        "uncoveredIntervalsOverflowed": true
                    }
                '''.replace(" ", "").replace("\n", "")

        DruidServiceConfig druidServiceConfig = Mock(DruidServiceConfig)
        druidServiceConfig.getTimeout() >> 100
        druidServiceConfig.getUrl() >> "url"
        AsyncDruidWebServiceImplV2 asyncDruidWebServiceImplV2 = new AsyncDruidWebServiceImplV2(
                druidServiceConfig,
                new ObjectMapper(),
                Mock(Supplier)
        )

        expect:
        asyncDruidWebServiceImplV2.constructJsonResponse(response).toString().replaceAll("\\\\", "") ==
                '''
                    {
                        "response": "[{"k1":"v1"}]",
                        "X-Druid-Response-Context": "{
                            "uncoveredIntervals": [
                                "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                                "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                            ],
                            "uncoveredIntervalsOverflowed": true
                        }"
                    }
                '''.replace(" ", "").replace("\n", "")
    }
}
