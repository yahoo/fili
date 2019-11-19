// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig

import org.asynchttpclient.Response

import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.util.function.Supplier

class HeaderNestingJsonBuilderStrategySpec extends Specification {

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
        response.getStatusCode() >> 200

        DruidServiceConfig druidServiceConfig = Mock(DruidServiceConfig)
        druidServiceConfig.getTimeout() >> 100
        druidServiceConfig.getUrl() >> "url"
        AsyncDruidWebServiceImpl asyncDruidWebServiceImplV2 = new AsyncDruidWebServiceImpl(
                druidServiceConfig,
                new ObjectMappersSuite().getMapper(),
                Mock(Supplier),
                new HeaderNestingJsonBuilderStrategy(AsyncDruidWebServiceImpl.DEFAULT_JSON_NODE_BUILDER_STRATEGY)
        )

        expect:
        asyncDruidWebServiceImplV2.jsonNodeBuilderStrategy.apply(response).toString().replaceAll("\\\\", "") ==
                '''
                    {
                        "response": [{"k1":"v1"}],
                        "X-Druid-Response-Context": {
                            "uncoveredIntervals": [
                                "2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
                                "2016-12-25T00:00:00.000Z/2017-01-03T00:00:00.000Z"
                            ],
                            "uncoveredIntervalsOverflowed": true
                        },
                        "status-code": 200
                    }
                '''.replace(" ", "").replace("\n", "")
    }
}
