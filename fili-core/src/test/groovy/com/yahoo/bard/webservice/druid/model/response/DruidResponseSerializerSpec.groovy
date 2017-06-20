// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response

import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.GROUP_BY
import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.TIMESERIES
import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.TOP_N
import static java.util.Arrays.asList

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTime

import spock.lang.Specification
import spock.lang.Unroll

class DruidResponseSerializerSpec extends Specification {
    static String TIME = "2017-06-13T00:00:00.000Z"
    static DateTime DATE_TIME = DateTime.parse(TIME);
    static ObjectMapper objectMapper = new ObjectMapper();
    static key1 = "key1"
    static key2 = "key2"


    @Unroll
    def "Serialize #type with #keys and #values"() {
        setup:
        DruidResponse druidResponse = DruidResponseFactory.getResponse(type);
        DruidResultRow row = DruidResponseFactory.getResultRow(type, DATE_TIME)
        for (int i = 0; i < keys.size(); i++) {
            row.add(keys.get(i), values.get(i))
        }
        druidResponse.add(row)

        expect:
        objectMapper.writeValueAsString(druidResponse) == serialized

        where:
        type       | keys               | values          | serialized

        TIMESERIES | asList(key1)       | asList("")      | """[{"timestamp":"${TIME}","result":{"key1":""}}]"""
        TIMESERIES | asList(key1)       | asList("value") | """[{"timestamp":"${TIME}","result":{"key1":"value"}}]"""
        TIMESERIES | asList(key1)       | asList(1)       | """[{"timestamp":"${TIME}","result":{"key1":1}}]"""
        TIMESERIES | asList(key1)       | asList(1.0D)    | """[{"timestamp":"${TIME}","result":{"key1":1.0}}]"""
        TIMESERIES | asList(key1, key2) | asList(1, 2)    | """[{"timestamp":"${TIME}","result":{"key1":1,"key2":2}}]"""

        GROUP_BY   | asList(key1)       | asList("")      | """[{"version":"v1","timestamp":"${TIME}","event":{"key1":""}}]"""
        GROUP_BY   | asList(key1)       | asList("value") | """[{"version":"v1","timestamp":"${TIME}","event":{"key1":"value"}}]"""
        GROUP_BY   | asList(key1)       | asList(1)       | """[{"version":"v1","timestamp":"${TIME}","event":{"key1":1}}]"""
        GROUP_BY   | asList(key1)       | asList(1.0D)    | """[{"version":"v1","timestamp":"${TIME}","event":{"key1":1.0}}]"""
        GROUP_BY   | asList(key1, key2) | asList(1, 2)    | """[{"version":"v1","timestamp":"${TIME}","event":{"key1":1,"key2":2}}]"""

        TOP_N      | asList(key1)       | asList("")      | """[{"timestamp":"${TIME}","result":[{"key1":""}]}]"""
        TOP_N      | asList(key1)       | asList("value") | """[{"timestamp":"${TIME}","result":[{"key1":"value"}]}]"""
        TOP_N      | asList(key1)       | asList(1)       | """[{"timestamp":"${TIME}","result":[{"key1":1}]}]"""
        TOP_N      | asList(key1)       | asList(1.0D)    | """[{"timestamp":"${TIME}","result":[{"key1":1.0}]}]"""
        TOP_N      | asList(key1, key2) | asList(1, 2)    | """[{"timestamp":"${TIME}","result":[{"key1":1,"key2":2}]}]"""
    }

    @Unroll
    def "Serialize TopN with combining timestamps #timeStamp"() {
        setup:
        def type = TOP_N
        DruidResponse druidResponse = DruidResponseFactory.getResponse(type)
        for (int i = 0; i < timeStamp.size(); i++) {
            DruidResultRow row = DruidResponseFactory.getResultRow(type, timeStamp.get(i))
            row.add(keys.get(i), values.get(i))
            druidResponse.add(row)
        }

        expect:
        objectMapper.writeValueAsString(druidResponse) == serialized
        timeStamp.size() == keys.size() && keys.size() == values.size()

        where:
        timeStamp                    | keys               | values       | serialized
        asList(DATE_TIME)            | asList(key1)       | asList(1)    | """[{"timestamp":"${TIME}","result":[{"key1":1}]}]"""
        asList(DATE_TIME, DATE_TIME) | asList(key1, key2) | asList(1, 2) | """[{"timestamp":"${TIME}","result":[{"key1":1},{"key2":2}]}]"""
    }
}
