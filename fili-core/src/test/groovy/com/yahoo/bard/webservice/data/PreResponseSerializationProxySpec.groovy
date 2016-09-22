// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
/**
 * preResponse object serialization test
 */
class PreResponseSerializationProxySpec extends Specification {

    ObjectMappersSuite objectMappersSuite = new ObjectMappersSuite()
    SerializationResources resources
    PreResponseSerializationProxy preResponseProxy
    ObjectMapper typePreservingMapper

    def setup() {
        resources = new SerializationResources().init()
        ObjectMappersSuite MapperSuite = new ObjectMappersSuite()
        typePreservingMapper = MapperSuite.getMapper()
        typePreservingMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        preResponseProxy = new PreResponseSerializationProxy(resources.preResponse, typePreservingMapper)
    }

    def "Validate PreResponse custom serialization"() {
       expect:
        GroovyTestUtils.compareJson(
                resources.serializedPreResponse,
                objectMappersSuite.mapper.writeValueAsString(preResponseProxy),
                JsonSortStrategy.SORT_BOTH)
    }

    def "Validate ResponseContext custom serialization" () {
        setup:

        String serializedResponseContext = preResponseProxy.getSerializedResponseContext(
                resources.responseContext,
                typePreservingMapper
        )
        ResponseContext responseContext = typePreservingMapper.readValue(
                serializedResponseContext,
                ResponseContext.class
        )

        expect:
        responseContext.get("missingIntervals") == [
                "a",
                "b",
                "c",
                new SimplifiedIntervalList([resources.interval]),
                resources.bigDecimal
        ]
        responseContext.get("randomHeader") == "someHeader"
    }

    def "validate that ResponseContext with apiMetricColumnNames and requestedApiDimensionFields serializes correctly" () {
        setup:
        String serializedResponseContext = preResponseProxy.getSerializedResponseContext(
                resources.responseContext1,
                typePreservingMapper
        )

        expect:
        serializedResponseContext == resources.serializedReponseContext1
    }
}
