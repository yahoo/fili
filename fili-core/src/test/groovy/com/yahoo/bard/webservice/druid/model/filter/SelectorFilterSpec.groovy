package com.yahoo.bard.webservice.druid.model.filter

import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.RegisteredLookupExtractionFunction

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

/**
 * Test selector filter serialization.
 */
class SelectorFilterSpec extends Specification{

    ObjectMapper objectMapper

    // Dimension missing due to lack of proper way to inject mock dimension value and therefore null is given
    String expectedSerialization =
            """
                {
                    "type":"selector",
                    "extractionFn":{
                        "type":"registeredLookup",
                        "lookup":"lookup",
                        "retainMissingValue":false,
                        "replaceMissingValueWith":"none",
                        "injective":false,
                        "optimize":false
                    },
                    "value":"value"
                }
            """

    def setup() {
        objectMapper = new ObjectMapper()
    }

    def "Test dimensional filter serialize as expected by druid"() {
        given:
        SelectorFilter filter = new SelectorFilter(null, "value", new RegisteredLookupExtractionFunction("lookup", false, "none", false, false))
        String serializedFilter = objectMapper.writeValueAsString(filter)

        expect:
        objectMapper.readTree(serializedFilter) == objectMapper.readTree(expectedSerialization)
    }
}
