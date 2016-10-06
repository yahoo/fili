// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import com.yahoo.bard.webservice.druid.model.QueryType
import com.yahoo.bard.webservice.table.PhysicalTable

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.table.ZonedSchema

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DruidResponseParserSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    @Shared DimensionDictionary dimensionDictionary

    def setup() {
        def dimensionNames = [
                "ageBracket",
                "gender",
                "unknown"
        ]

        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        dimensionDictionary = new DimensionDictionary()
        KeyValueStoreDimension dimension
        for (String name : dimensionNames) {
            dimension = new KeyValueStoreDimension(
                    name,
                    name,
                    dimensionFields,
                    MapStoreManager.getInstance("dimension"),
                    ScanSearchProviderManager.getInstance(name)
            )
            dimension.setLastUpdated(new DateTime(10000))
            dimensionDictionary.add(dimension)
        }

        dimensionDictionary.findByApiName("ageBracket").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "1", "1"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "4", "4"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "", "unknown"))
        }

        dimensionDictionary.findByApiName("gender").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "u", "u"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "f", "u"))
        }
    }

    def "parse group by with numeric metrics only into a ResultSet"() {
        given:

        String jsonText = """
        [ {
            "version" : "v1",
            "timestamp" : "2012-01-01T00:00:00.000Z",
            "event" : {
                "ageBracket" : "4",
                "gender" : "u",
                "unknown" : "foo",
                "pageViews" : 1,
                "time_spent" : 2,
                "TSDPBV" : 3
            }
        }, {
            "version" : "v1",
            "timestamp" : "2012-01-01T00:00:00.000Z",
            "event" : {
                "ageBracket" : "1",
                "gender" : "f",
                "unknown" : "foo",
                "pageViews" : 101,
                "time_spent" : 102,
                "sample_divide" : "some_other_sample_divide_value"
            }
        }, {
            "version" : "v1",
            "timestamp" : "2012-01-01T00:00:00.000Z",
            "event" : {
                "ageBracket" : null,
                "gender" : "f",
                "unknown" : "foo",
                "pageViews" : 101,
                "time_spent" : 102,
                "sample_divide" : "some_other_sample_divide_value"
            }
        }
         ]
        """

        JsonParser parser = new JsonFactory().createParser(jsonText)
        JsonNode jsonResult = MAPPER.readTree(parser)

        Schema schema = buildSchema(["pageViews", "time_spent"])
        ResultSet resultSet = new DruidResponseParser().parse(jsonResult, schema, DefaultQueryType.GROUP_BY)

        expect:
        resultSet != null
        resultSet.size() == 3
        resultSet.getSchema() == schema

        and:
        Result result = resultSet.get(0)
        result.getDimensionRow(schema.columns.toArray()[0])?.get(BardDimensionField.DESC) == "4"
        result.getDimensionRow(schema.columns.toArray()[1])?.get(BardDimensionField.DESC) == "u"
        result.getDimensionRow(schema.columns.toArray()[2])?.get(BardDimensionField.DESC) == ""
        result.getDimensionRow(schema.columns.toArray()[2])?.get(BardDimensionField.ID) == "foo"
        resultSet[0].getMetricValueAsNumber(schema.getColumn("pageViews")) == 1 as BigDecimal
        resultSet[0].getMetricValueAsNumber(schema.getColumn("time_spent")) == 2 as BigDecimal

        and:
        Result resultWithNullDimensionKey = resultSet.get(2)
        resultWithNullDimensionKey.getDimensionRow(schema.columns.toArray()[0])?.get(BardDimensionField.ID) == ""
        resultWithNullDimensionKey.getDimensionRow(schema.columns.toArray()[0])?.get(BardDimensionField.DESC) ==
                "unknown"
    }

    def "parse sample top N ResultSet with only numeric metrics"() {
        given:

        String jsonText = """
        [ {
            "timestamp" : "2012-01-01T00:00:00.000Z",
            "result" : [ {
                "ageBracket" : "4",
                "pageViews" : 1
            } ]
        }, {
            "timestamp" : "2012-01-04T00:00:00.000Z",
            "result" : [ {
                "ageBracket" : "1",
                "pageViews" : 101
            } ]
        }, {
            "timestamp" : "2012-01-04T00:00:00.000Z",
            "result" : [ {
                "ageBracket" : null,
                "pageViews" : 101
            } ]
        }
         ]
        """

        JsonParser parser = new JsonFactory().createParser(jsonText)
        JsonNode jsonResult = MAPPER.readTree(parser)

        /* build Schema */
        ZonedSchema schema = new ZonedSchema(DAY, DateTimeZone.UTC)
        DimensionColumn.addNewDimensionColumn(schema, dimensionDictionary.findByApiName("ageBracket"), new PhysicalTable("null", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        MetricColumn.addNewMetricColumn(schema, "pageViews")
        ResultSet resultSet = new DruidResponseParser().parse(jsonResult, schema, DefaultQueryType.TOP_N)

        expect:
        resultSet != null
        resultSet.size() == 3
        resultSet.getSchema() == schema

        and:
        Result result = resultSet.get(0)
        result.getDimensionRow(schema.columns.toArray()[0])?.get(BardDimensionField.DESC) == "4"

        and:
        Result resultWithNullDimensionKey = resultSet.get(2)
        resultWithNullDimensionKey.getDimensionRow(schema.columns.toArray()[0])?.get(BardDimensionField.ID) == ""
        resultWithNullDimensionKey.getDimensionRow(schema.columns.toArray()[0])?.get(BardDimensionField.DESC) ==
                "unknown"
    }

    def "parse lookback with timeseries datasource into a ResultSet"() {
        given:

        String jsonText = """
        [ {
            "timestamp" : "2012-01-01T00:00:00.000Z",
            "result" :  {
                "pageViews" : 1,
                "lookback_pageViews" : 2,
                "retentionPageViews": 1
            }
        }, {
            "timestamp" : "2012-01-02T00:00:00.000Z",
            "result" :  {
                "pageViews" : 1,
                "lookback_pageViews" : 2,
                "retentionPageViews": 1
            }
        } ]
        """

        JsonParser parser = new JsonFactory().createParser(jsonText)
        JsonNode jsonResult = MAPPER.readTree(parser)

        /* build Schema */
        ZonedSchema schema = new ZonedSchema(DAY, DateTimeZone.UTC)
        MetricColumn.addNewMetricColumn(schema, "pageViews")
        MetricColumn.addNewMetricColumn(schema, "lookback_pageViews")
        MetricColumn.addNewMetricColumn(schema, "retentionPageViews")
        ResultSet resultSet = new DruidResponseParser().parse(jsonResult, schema, DefaultQueryType.LOOKBACK)

        expect:
        resultSet != null
        resultSet.size() == 2
        resultSet.getSchema() == schema

        and:
        resultSet[0].getMetricValueAsNumber(schema.getColumn("retentionPageViews")) == 1 as BigDecimal
        resultSet[0].getMetricValueAsNumber(schema.getColumn("lookback_pageViews")) == 2 as BigDecimal
        resultSet[0].getMetricValueAsNumber(schema.getColumn("pageViews")) == 1 as BigDecimal
    }

    def "parse lookback with groupBy datasource into a Resultset"() {
        given:

        String jsonText = """
        [ {
            "version" : "v1",
            "timestamp" : "2016-01-12T00:00:00.000Z",
            "event" : {
            "ageBracket" : "4",
            "gender" : "u",
            "unknown" : "foo",
            "pageViews" : 1,
            "lookback_pageViews" : 2,
            "retentionPageViews" : 1
            }
        }, {
            "version" : "v1",
            "timestamp" : "2016-01-12T00:00:00.000Z",
            "event" : {
            "ageBracket" : "1",
            "gender" : "f",
            "unknown" : "foo",
            "pageViews" : 1,
            "lookback_pageViews" : 2,
            "retentionPageViews" : 1
            }
        }]
        """

        JsonParser parser = new JsonFactory().createParser(jsonText)
        JsonNode jsonResult = MAPPER.readTree(parser)

        Schema schema = buildSchema(["pageViews", "lookback_pageViews", "retentionPageViews"])
        ResultSet resultSet = new DruidResponseParser().parse(jsonResult, schema, DefaultQueryType.GROUP_BY)

        expect:
        resultSet != null
        resultSet.size() == 2
        resultSet.getSchema() == schema

        and:
        Result result = resultSet.get(0)
        result.getDimensionRow(schema.columns.toArray()[0])?.get(BardDimensionField.DESC) == "4"
        result.getDimensionRow(schema.columns.toArray()[1])?.get(BardDimensionField.DESC) == "u"
        result.getDimensionRow(schema.columns.toArray()[2])?.get(BardDimensionField.DESC) == ""
        result.getDimensionRow(schema.columns.toArray()[2])?.get(BardDimensionField.ID) == "foo"
        resultSet[0].getMetricValueAsNumber(schema.getColumn("pageViews")) == 1 as BigDecimal
        resultSet[0].getMetricValueAsNumber(schema.getColumn("lookback_pageViews")) == 2 as BigDecimal
        resultSet[0].getMetricValueAsNumber(schema.getColumn("retentionPageViews")) == 1 as BigDecimal
    }

    @Unroll
    def "A Druid #queryType result containing String metrics is parsed correctly"() {
        given: "A simple response from Druid containing String metrics luckyNumbers and unluckyNumbers."
        String druidResponse = buildResponse(queryType, ['"luckyNumbers"':'"1, 3, 7"', '"unluckyNumbers"': '"2"'])

        when: "We build a result set from the Druid response"
        ZonedSchema schema = buildSchema(["luckyNumbers", "unluckyNumbers"])
        ResultSet resultSet = buildResultSet(druidResponse, schema, queryType)

        then: "The result set was built correctly"
        resultSet != null
        resultSet.size() == 1
        resultSet.getSchema() == schema

        and: "The metrics were parsed correctly from the Druid response"
        resultSet.get(0).getMetricValueAsString(schema.getColumn("luckyNumbers") as MetricColumn) == "1, 3, 7"
        resultSet.get(0).getMetricValueAsString(schema.getColumn("unluckyNumbers") as MetricColumn) == "2"

        where:
        queryType << [DefaultQueryType.GROUP_BY, DefaultQueryType.TOP_N, DefaultQueryType.TIMESERIES]
    }


    @Unroll
    def "A Druid #queryType result containing boolean metrics is parsed correctly"(){
        given: "A simple response from Druid containing boolean metrics true and false."
        String druidResponse = buildResponse(queryType, ['"true"': true, '"false"': false])

        when: "We build a result set from the Druid response"
        ZonedSchema schema = buildSchema(["true", "false"])
        ResultSet resultSet = buildResultSet(druidResponse, schema, queryType)

        then: "The result set was built correctly"
        resultSet != null
        resultSet.size() == 1
        resultSet.getSchema() == schema

        and: "The metrics were parsed correctly from the Druid response"
        resultSet.get(0).getMetricValueAsBoolean(schema.getColumn("true", MetricColumn.class))
        !resultSet.get(0).getMetricValueAsBoolean(schema.getColumn("false", MetricColumn.class))

        where:
        queryType << [DefaultQueryType.GROUP_BY, DefaultQueryType.TOP_N, DefaultQueryType.TIMESERIES]
    }

    @Unroll
    def "A Druid #queryType result containing a null metric is parsed correctly"(){
        given: "A simple response from Druid containing a null metric 'null'."
        String druidResponse = buildResponse(queryType, ['"null"': null])

        when: "We try to build a result set from the Druid response"
        ZonedSchema schema = buildSchema(["null"])
        ResultSet resultSet = buildResultSet(druidResponse, schema, queryType)

        then: "The result set was built correctly"
        resultSet != null
        resultSet.size() == 1
        resultSet.getSchema() == schema

        and: "The metrics were parsed correctly from the Druid response"
        resultSet.get(0).getMetricValue(schema.getColumn("null", MetricColumn.class)) == null

        where:
        queryType << [DefaultQueryType.GROUP_BY, DefaultQueryType.TOP_N, DefaultQueryType.TIMESERIES]
    }

    @Unroll
    def "A Druid #queryType result containing a JsonNode metric is parsed correctly"(){
        given: "A simple response from Druid containing a few JsonNode metrics luckyNumbers and unluckyNumbers."
        String luckyNumberNode = '{"values": "1, 3, 7", "length": 3}'
        String unluckyNumberNode = '{"values": "2", "length": 1}'
        String druidResponse = buildResponse(
                queryType,
                [
                        '"luckyNumbers"': luckyNumberNode,
                        '"unluckyNumbers"': unluckyNumberNode
                ]
        )

        when: "We try to build a result set from the Druid response"
        ZonedSchema schema = buildSchema(["luckyNumbers", "unluckyNumbers"])
        ResultSet resultSet = buildResultSet(druidResponse, schema, queryType)

        then: "The result set was built correctly"
        resultSet != null
        resultSet.size() == 1
        resultSet.getSchema() == schema

        and: "The metrics were parsed correctly from the Druid response"
        resultSet.get(0).getMetricValueAsJsonNode(schema.getColumn("luckyNumbers", MetricColumn.class)) ==
                MAPPER.readTree(luckyNumberNode)
        resultSet.get(0).getMetricValueAsJsonNode(schema.getColumn("unluckyNumbers", MetricColumn.class)) ==
                MAPPER.readTree(unluckyNumberNode)

        where:
        queryType << [DefaultQueryType.GROUP_BY, DefaultQueryType.TOP_N, DefaultQueryType.TIMESERIES]
    }

    def "Attempting to parse an unknown query type throws an UnsupportedOperationException"() {
        given:
        DruidResponseParser responseParser = new DruidResponseParser()
        QueryType mysteryType = Mock(QueryType)

        when:
        responseParser.parse(MAPPER.readTree("[]"), Mock(ZonedSchema), mysteryType)

        then:
        thrown(UnsupportedOperationException)

    }

    String buildResponse(DefaultQueryType queryType, Map complexMetrics) {
        //Strip off the brackets from the String representation of the Map.
        String complexMetricsString = complexMetrics.toString()[1..-2]
        if (queryType == DefaultQueryType.GROUP_BY) {
            return """
                [ {
                    "version" : "v1",
                    "timestamp" : "2012-01-01T00:00:00.000Z",
                    "event" : {
                        "ageBracket" : "4",
                        "gender" : "u",
                        $complexMetricsString
                    }
                } ]
            """
        } else if (queryType == DefaultQueryType.TOP_N) {
            return """
                [ {
                    "timestamp" : "2012-01-01T00:00:00.000Z",
                    "result" : [ {
                        "ageBracket" : "4",
                        $complexMetricsString
                    } ]
                } ]
            """
        } else {
            return """
                [ {
                    "timestamp" : "2012-01-01T00:00:00.000Z",
                    "result" : {
                        $complexMetricsString
                    }
                } ]
            """
        }
    }

    ResultSet buildResultSet(String druidResponse, ZonedSchema schema, DefaultQueryType queryType) {
        JsonNode jsonResult = MAPPER.readTree(new JsonFactory().createParser(druidResponse))
        return new DruidResponseParser().parse(jsonResult, schema, queryType)
    }

    ZonedSchema buildSchema(List<String> metricNames) {
        Schema schema = new ZonedSchema(DAY, DateTimeZone.UTC)
        DimensionColumn.addNewDimensionColumn(schema, dimensionDictionary.findByApiName("ageBracket"), new PhysicalTable("null", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        DimensionColumn.addNewDimensionColumn(schema, dimensionDictionary.findByApiName("gender"), new PhysicalTable("null", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        DimensionColumn.addNewDimensionColumn(schema, dimensionDictionary.findByApiName("unknown"), new PhysicalTable("null", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        metricNames.each {
            MetricColumn.addNewMetricColumn(schema, it)
        }
        return schema
    }
}
