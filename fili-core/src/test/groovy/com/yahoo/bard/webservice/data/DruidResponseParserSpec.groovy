// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.QueryType
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.Schema

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Stream

class DruidResponseParserSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    DimensionDictionary dimensionDictionary

    Set<DimensionColumn> dimensionColumns
    DimensionColumn ageColumn
    DimensionColumn genderColumn
    DimensionColumn unknownColumn
    DruidResponseParser responseParser

    def setup() {
        responseParser = new DruidResponseParser()

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

        ageColumn = new DimensionColumn(dimensionDictionary.findByApiName("ageBracket"))
        genderColumn = new DimensionColumn(dimensionDictionary.findByApiName("gender"))
        unknownColumn = new DimensionColumn(dimensionDictionary.findByApiName("unknown"))
        dimensionColumns = [ ageColumn, genderColumn, unknownColumn]
    }

    def "parse group by with numeric metrics only into a ResultSet"() {
        given:
        DimensionField description = BardDimensionField.DESC

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
        Column pageViewsColumn = schema.getColumn("pageViews", MetricColumn.class).get()
        Column timeSpentColumn = schema.getColumn("time_spent", MetricColumn.class).get()

        ResultSet resultSet = responseParser.parse(jsonResult, schema, DefaultQueryType.GROUP_BY, DateTimeZone.UTC)

        expect:
        resultSet != null
        resultSet.size() == 3
        resultSet.getSchema() == schema

        and:
        Result firstResult = resultSet.get(0)
        firstResult.getDimensionRow(genderColumn)?.get(description) == "u"
        firstResult.getDimensionRow(ageColumn)?.get(description) == "4"
        firstResult.getDimensionRow(unknownColumn)?.get(description) == ""
        firstResult.getDimensionRow(unknownColumn)?.get(BardDimensionField.ID) == "foo"
        firstResult.getMetricValueAsNumber(pageViewsColumn) == 1 as BigDecimal
        firstResult.getMetricValueAsNumber(timeSpentColumn) == 2 as BigDecimal

        and:
        Result resultWithNullDimensionKey = resultSet.get(2)
        resultWithNullDimensionKey.getDimensionRow(ageColumn)?.get(BardDimensionField.ID) == ""
        resultWithNullDimensionKey.getDimensionRow(ageColumn)?.get(BardDimensionField.DESC) == "unknown"
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
        ResultSetSchema schema = new ResultSetSchema(DAY, [new DimensionColumn(dimensionDictionary.findByApiName("ageBracket")), new MetricColumn("pageViews")].toSet())
        ResultSet resultSet = responseParser.parse(jsonResult, schema, DefaultQueryType.TOP_N, DateTimeZone.UTC)

        expect:
        resultSet != null
        resultSet.size() == 3
        resultSet.getSchema() == schema

        and:
        Result result = resultSet.get(0)
        result.getDimensionRow((DimensionColumn) schema.columns.toArray()[1])?.get(BardDimensionField.DESC) == "4"

        and:
        Result resultWithNullDimensionKey = resultSet.get(2)
        resultWithNullDimensionKey.getDimensionRow((DimensionColumn) schema.columns.toArray()[1])?.get(BardDimensionField.ID) == ""
        resultWithNullDimensionKey.getDimensionRow((DimensionColumn) schema.columns.toArray()[1])?.get(BardDimensionField.DESC) ==
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
        ResultSetSchema schema = new ResultSetSchema(DAY, [new MetricColumn("pageViews"), new MetricColumn("lookback_pageViews"), new MetricColumn("retentionPageViews")].toSet())
        ResultSet resultSet = responseParser.parse(jsonResult, schema, DefaultQueryType.LOOKBACK, DateTimeZone.UTC)

        expect:
        resultSet != null
        resultSet.size() == 2
        resultSet.getSchema() == schema

        and:
        resultSet[0].getMetricValueAsNumber(schema.getColumn("retentionPageViews", MetricColumn.class).get()) == 1 as BigDecimal
        resultSet[0].getMetricValueAsNumber(schema.getColumn("lookback_pageViews", MetricColumn.class).get()) == 2 as BigDecimal
        resultSet[0].getMetricValueAsNumber(schema.getColumn("pageViews", MetricColumn.class).get()) == 1 as BigDecimal
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
        ResultSet resultSet = responseParser.parse(jsonResult, schema, DefaultQueryType.GROUP_BY, DateTimeZone.UTC)
        Column pageViewsColumn = schema.getColumn("pageViews", MetricColumn.class).get()
        Column lookbackPageviewsColumn = schema.getColumn("lookback_pageViews", MetricColumn.class).get()
        Column retentionPageviewsColumn = schema.getColumn("retentionPageViews", MetricColumn.class).get()

        expect:
        resultSet != null
        resultSet.size() == 2
        resultSet.getSchema() == schema

        and:
        Result firstResult = resultSet.get(0)
        firstResult.getDimensionRow(genderColumn)?.get(BardDimensionField.DESC) == "u"
        firstResult.getDimensionRow(ageColumn)?.get(BardDimensionField.DESC) == "4"
        firstResult.getDimensionRow(unknownColumn)?.get(BardDimensionField.DESC) == ""
        firstResult.getDimensionRow(unknownColumn)?.get(BardDimensionField.ID) == "foo"
        firstResult.getMetricValueAsNumber(pageViewsColumn) == 1 as BigDecimal
        firstResult.getMetricValueAsNumber(lookbackPageviewsColumn) == 2 as BigDecimal
        firstResult.getMetricValueAsNumber(retentionPageviewsColumn) == 1 as BigDecimal
    }

    @Unroll
    def "A Druid #queryType result containing String metrics is parsed correctly"() {
        given: "A simple response from Druid containing String metrics luckyNumbers and unluckyNumbers."
        String druidResponse = buildResponse(queryType, ['"luckyNumbers"':'"1, 3, 7"', '"unluckyNumbers"': '"2"'])

        when: "We build a result set from the Druid response"
        ResultSetSchema schema = buildSchema(["luckyNumbers", "unluckyNumbers"])
        ResultSet resultSet = buildResultSet(druidResponse, schema, queryType)

        then: "The result set was built correctly"
        resultSet != null
        resultSet.size() == 1
        resultSet.getSchema() == schema

        and: "The metrics were parsed correctly from the Druid response"
        resultSet.get(0).getMetricValueAsString(schema.getColumn("luckyNumbers", MetricColumn.class).get() as MetricColumn) == "1, 3, 7"
        resultSet.get(0).getMetricValueAsString(schema.getColumn("unluckyNumbers", MetricColumn.class).get() as MetricColumn) == "2"

        where:
        queryType << [DefaultQueryType.GROUP_BY, DefaultQueryType.TOP_N, DefaultQueryType.TIMESERIES]
    }


    @Unroll
    def "A Druid #queryType result containing boolean metrics is parsed correctly"(){
        given: "A simple response from Druid containing boolean metrics true and false."
        String druidResponse = buildResponse(queryType, ['"true"': true, '"false"': false])

        when: "We build a result set from the Druid response"
        ResultSetSchema schema = buildSchema(["true", "false"])
        ResultSet resultSet = buildResultSet(druidResponse, schema, queryType)

        then: "The result set was built correctly"
        resultSet != null
        resultSet.size() == 1
        resultSet.getSchema() == schema

        and: "The metrics were parsed correctly from the Druid response"
        resultSet.get(0).getMetricValueAsBoolean(schema.getColumn("true", MetricColumn.class).get())
        !resultSet.get(0).getMetricValueAsBoolean(schema.getColumn("false", MetricColumn.class).get())

        where:
        queryType << [DefaultQueryType.GROUP_BY, DefaultQueryType.TOP_N, DefaultQueryType.TIMESERIES]
    }

    @Unroll
    def "A Druid #queryType result containing a null metric is parsed correctly"(){
        given: "A simple response from Druid containing a null metric 'null'."
        String druidResponse = buildResponse(queryType, ['"null"': null])

        when: "We try to build a result set from the Druid response"
        ResultSetSchema schema = buildSchema(["null"])
        ResultSet resultSet = buildResultSet(druidResponse, schema, queryType)

        then: "The result set was built correctly"
        resultSet != null
        resultSet.size() == 1
        resultSet.getSchema() == schema

        and: "The metrics were parsed correctly from the Druid response"
        resultSet.get(0).getMetricValue(schema.getColumn("null", MetricColumn.class).get()) == null

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
        ResultSetSchema schema = buildSchema(["luckyNumbers", "unluckyNumbers"])
        ResultSet resultSet = buildResultSet(druidResponse, schema, queryType)

        then: "The result set was built correctly"
        resultSet != null
        resultSet.size() == 1
        resultSet.getSchema() == schema

        and: "The metrics were parsed correctly from the Druid response"
        resultSet.get(0).getMetricValueAsJsonNode(schema.getColumn("luckyNumbers", MetricColumn.class).get()) ==
                MAPPER.readTree(luckyNumberNode)
        resultSet.get(0).getMetricValueAsJsonNode(schema.getColumn("unluckyNumbers", MetricColumn.class).get()) ==
                MAPPER.readTree(unluckyNumberNode)

        where:
        queryType << [DefaultQueryType.GROUP_BY, DefaultQueryType.TOP_N, DefaultQueryType.TIMESERIES]
    }

    def "Attempting to parse an unknown query type throws an UnsupportedOperationException"() {
        given:
        QueryType mysteryType = Mock(QueryType)

        when:
        responseParser.parse(MAPPER.readTree("[]"), Mock(ResultSetSchema), mysteryType, DateTimeZone.UTC)

        then:
        thrown(UnsupportedOperationException)
    }

    def "Druid response parser delegates to query for schema columns"() {
        setup:
        Stream<Column> columnStream = Mock(Stream)
        DruidAggregationQuery query = Mock(DruidAggregationQuery)
        1 * query.buildSchemaColumns() >> columnStream

        expect:
        responseParser.buildSchemaColumns(query) == columnStream
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

    ResultSet buildResultSet(String druidResponse, ResultSetSchema schema, DefaultQueryType queryType) {
        JsonNode jsonResult = MAPPER.readTree(new JsonFactory().createParser(druidResponse))
        return responseParser.parse(jsonResult, schema, queryType, DateTimeZone.UTC)
    }

    ResultSetSchema buildSchema(List<String> metricNames) {
        metricNames.each {
            dimensionColumns.add(new MetricColumn(it))
        }
        new ResultSetSchema(DAY, dimensionColumns)
    }
}
