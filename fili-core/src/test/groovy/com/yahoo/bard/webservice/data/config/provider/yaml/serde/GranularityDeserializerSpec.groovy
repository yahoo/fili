// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml.serde

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.Granularity
import spock.lang.Specification
import spock.lang.Unroll

public class GranularityDeserializerSpec extends Specification {

    def "Invalid granularity should throw exception"() {
        setup:
        ObjectMapper mapper = DeserializationHelper.getMapper(Granularity, new GranularityDeserializer())
        when:
        mapper.readValue('"something bad"', Granularity)
        then:

        JsonParseException ex = thrown()
        ex.message =~ /Unable to parse granularity.*something bad.*/
    }

    def "Deserializing all granularity should return the correct value"() {
        setup:
        ObjectMapper mapper = DeserializationHelper.getMapper(Granularity, new GranularityDeserializer())
        def gran = mapper.readValue('"all"', Granularity)
        expect:
        gran == AllGranularity.INSTANCE
    }

    @Unroll
    def "Deserializing default granularities should work"(Granularity expected, String name) {
        setup:
        ObjectMapper mapper = DeserializationHelper.getMapper(Granularity, new GranularityDeserializer())
        def gran = mapper.readValue("\"${name}\"", Granularity)

        expect:
        gran == expected

        where:
        [expected, name] << Arrays.stream(DefaultTimeGrain.values()).map( { a -> [a, a.toString()]})
    }
}
