// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.table.ZonedSchema
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.web.PreResponse
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class PreResponseDeSerializationSpec extends Specification {

    @Shared SerializationResources resources
    PreResponseDeserializer preResponseDeSerializer
    ObjectMappersSuite objectMappers = new ObjectMappersSuite()
    ObjectMapper typePreservingMapper

    def setupSpec() {
        resources = new SerializationResources().init()
    }

    def setup() {
        ObjectMappersSuite MapperSuite = new ObjectMappersSuite()
        typePreservingMapper = MapperSuite.getMapper()
        typePreservingMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        preResponseDeSerializer = new PreResponseDeserializer(
                resources.dimensionDictionary,
                objectMappers.mapper,
                typePreservingMapper,
                new  StandardGranularityParser()
        )
    }

    def "PreResponse de-serialization from serialized PreResponse object validation"() {
        setup:
        PreResponse preResponseObj = preResponseDeSerializer.deserialize(
                resources.serializedPreResponse
        )

        expect:
        GroovyTestUtils.compareObjects(resources.resultSet,  preResponseObj.resultSet)
    }

    def "ResponseContext de-Serialization from serialized ResponseContext object validation"() {
        setup:

        ResponseContext responseContext = preResponseDeSerializer.getResponseContext(
                objectMappers.getMapper().readTree(resources.serializedResponseContext).get("responseContext")
        )

        expect:
        GroovyTestUtils.compareObjects(resources.responseContext,  responseContext)

    }

    def "Validate Result deserialization from resultSet succeeds"() {
        setup:
        ResultSet resultSet = preResponseDeSerializer.getResultSet(
                objectMappers.getMapper().readTree(resources.serializedResultSet)
        )

        expect:
        GroovyTestUtils.compareObjects(resources.resultSet,  resultSet)
    }

    def "ZonedSchema de-Serialization from serialized ZonedSchema object validation"() {
        setup:
        ZonedSchema zonedSchema = preResponseDeSerializer.getZonedSchema(
                objectMappers.getMapper().readTree(getSerializedZonedSchema())
        )

        expect:
        GroovyTestUtils.compareObjects(resources.schema,  zonedSchema)
    }

    def "Dimension's extraction from serialized Dimension rows validation"() {
        setup:
        LinkedHashMap<DimensionColumn, DimensionRow> dimensionRows = preResponseDeSerializer.extractDimensionValues(
                objectMappers.getMapper().readTree(getSerializedDimensionValues()),
                resources.schema.getColumns(DimensionColumn.class)
        )

        expect:
        GroovyTestUtils.compareObjects(resources.dimensionRows1,  dimensionRows)
    }

    @Unroll
    def "MetricColumn's extraction from serialized Metric columns validation"() {
        setup:
        Map<MetricColumn, Object> metricValues = preResponseDeSerializer.extractMetricValues(
                objectMappers.getMapper().readTree(serializedMetricValues),
                schema.getColumns(MetricColumn.class)
        )

        expect:
        GroovyTestUtils.compareObjects(expected,  metricValues)

        where:
        serializedMetricValues        | expected                 | schema
        getSerializedMetricValues1()  | resources.metricValues1  | resources.schema
        getSerializedMetricValues2()  | resources.metricValues4  | resources.schema
    }

    def "Validate metricColumn deserialization throws an exception for wrong json format"() {
        when:
        preResponseDeSerializer.extractMetricValues(
                objectMappers.getMapper().readTree(getSerializedMetricValues3()),
                resources.schema.getColumns(MetricColumn.class)
        )
        then:
        thrown(JsonParseException)
    }

    def "Validate metricColumn deserialization throws an exception for unexpected structure of valid json"() {
        when:
        preResponseDeSerializer.extractMetricValues(
                objectMappers.getMapper().readTree(getSerializedMetricValues4()),
                resources.schema.getColumns(MetricColumn.class)
        )
        then:
        thrown(DeserializationException)
    }


    @Unroll
    def "Result de-Serialization from a custom serialized Result object validation" () {
        setup:
        Result result = preResponseDeSerializer.getResult(objectMappers.getMapper().readTree(serialized), schema)

        expect:
        GroovyTestUtils.compareObjects(result, expected)

        where:
        serialized                  | schema            | expected
        resources.serializedResult2 | resources.schema  | resources.result2
        resources.serializedResult3 | resources.schema3 | resources.result3
    }

    String getSerializedZonedSchema(){
        """
        {
              "dimensionColumns": [
                "ageBracket",
                "gender",
                "country"
              ],
              "granularity": "day",
              "metricColumns": [
                "simplePageViews",
                "lookbackPageViews",
                "retentionPageViews"
              ],
              "metricColumnsType": {
                 "lookbackPageViews": "java.math.BigDecimal",
                 "retentionPageViews": "java.math.BigDecimal",
                 "simplePageViews": "java.math.BigDecimal"
              },
              "timeZone": "UTC"
        }
        """
    }

    String getSerializedDimensionValues(){
        """
            {
             "ageBracket": "1",
             "country": "US",
             "gender": "m"
            }
        """
    }

    String getSerializedMetricValues1(){
        """
            {
              "lookbackPageViews": 112,
              "retentionPageViews": 113,
              "simplePageViews": 111
            }
        """
    }

    String getSerializedMetricValues2(){
        """
            {
              "lookbackPageViews": null,
              "retentionPageViews": 113,
              "simplePageViews": 111
            }
        """
    }

    String getSerializedMetricValues3(){
        """
            {
              "lookbackPageViews"-null,
              "retentionPageViews": null,
              "simplePageViews": 333,
              "someMetric": 333
            }
        """
    }

    String getSerializedMetricValues4(){
        """
            [1,2,3,4,5]
        """
    }
}
