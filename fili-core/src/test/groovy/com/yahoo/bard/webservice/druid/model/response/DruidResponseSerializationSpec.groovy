package com.yahoo.bard.webservice.druid.model.response

import static com.yahoo.bard.webservice.druid.model.response.GroupByResultRow.Version.V1
import static com.yahoo.bard.webservice.druid.model.response.GroupByResultRow.Version.V2
import static java.util.Arrays.asList

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTime

import spock.lang.Specification
import spock.lang.Unroll

class DruidResponseSerializationSpec extends Specification {
    static DateTime DATE_TIME = DateTime.parse("2017-06-13T00:00:00.000Z");
    static ObjectMapper objectMapper = new ObjectMapper();
    static key1 = "key1"
    static key2 = "key2"

    @Unroll
    def "Check Timeseries druid response serialization"() {
        setup:
        DruidResponse druidResponse = new DruidResponse();
        TimeseriesResultRow row = new TimeseriesResultRow(DATE_TIME)
        for (int i = 0; i < keys.size(); i++) {
            row.add(keys.get(i), values.get(i))
        }
        druidResponse.add(row)

        expect:
        objectMapper.writeValueAsString(druidResponse) == serialized

        where:
        keys               | values          | serialized
        asList(key1)       | asList("value") | """[{"timestamp":"2017-06-13T00:00:00.000Z","result":{"key1":"value"}}]"""
        asList(key1)       | asList(1)       | """[{"timestamp":"2017-06-13T00:00:00.000Z","result":{"key1":1}}]"""
        asList(key1)       | asList(1.0D)    | """[{"timestamp":"2017-06-13T00:00:00.000Z","result":{"key1":1.0}}]"""
        asList(key1, key2) | asList(1, 2)    | """[{"timestamp":"2017-06-13T00:00:00.000Z","result":{"key1":1,"key2":2}}]"""
    }

    @Unroll
    def "Check GroupBy druid response serialization"() {
        setup:
        DruidResponse druidResponse = new DruidResponse();
        GroupByResultRow row = new GroupByResultRow(DATE_TIME, version)
        for (int i = 0; i < keys.size(); i++) {
            row.add(keys.get(i), values.get(i))
        }
        druidResponse.add(row)

        expect:
        objectMapper.writeValueAsString(druidResponse) == serialized

        where:
        keys               | values          | version | serialized
        asList(key1)       | asList("value") | V1      | """[{"version":"v1","timestamp":"2017-06-13T00:00:00.000Z","event":{"key1":"value"}}]"""
        asList(key1)       | asList(1)       | V1      | """[{"version":"v1","timestamp":"2017-06-13T00:00:00.000Z","event":{"key1":1}}]"""
        asList(key1)       | asList(1.0D)    | V2      | """[{"version":"v2","timestamp":"2017-06-13T00:00:00.000Z","event":{"key1":1.0}}]"""
        asList(key1, key2) | asList(1, 2)    | V1      | """[{"version":"v1","timestamp":"2017-06-13T00:00:00.000Z","event":{"key1":1,"key2":2}}]"""
    }
}
