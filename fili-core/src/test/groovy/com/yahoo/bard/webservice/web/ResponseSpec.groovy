// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.table.PhysicalTable

import static com.yahoo.bard.webservice.config.BardFeatureFlag.PARTIAL_DATA
import static com.yahoo.bard.webservice.util.SimplifiedIntervalList.NO_INTERVALS

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.util.Pagination
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.fasterxml.jackson.dataformat.csv.CsvSchema

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormatter

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.UriBuilder

class ResponseSpec extends Specification {

    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    SystemConfig systemConfig = SystemConfigProvider.getInstance()

    static final int PAGE = 2
    static final int PER_PAGE = 2
    static final String NEXT_PAGE = "example.yahoo.com:1234/v1/data/network/day/platform?metrics=pageViews&" +
            "timeSpent&perPage=$PER_PAGE&page=$PAGE"
    static final String PREVIOUS_PAGE = "example.yahoo.com:1234/v1/data/network/day/platform?metrics=pageViews&" +
            "timeSpent&perPage=$PER_PAGE&page=$PAGE"
    static final String FIRST_PAGE = "example.yahoo.com:1234/v1/data/network/day/platform?metrics=pageViews&" +
            "timeSpent&perPage=$PER_PAGE&page=1"
    static final String LAST_PAGE = "example.yahoo.com:1234/v1/data/network/day/platform?metrics=pageViews&" +
            "timeSpent&perPage=$PER_PAGE&page=3"

    static final List<String> LINK_NAMES_LIST = [
            "first, last, next, previous",
            "first, last, next",
            "first, last, previous",
            "first, last"
    ]

    static final List<Map<String, URI>> BODY_LINKS_LIST = [
            [
                    first: UriBuilder.fromUri(FIRST_PAGE).build(),
                    last: UriBuilder.fromUri(LAST_PAGE).build(),
                    next: UriBuilder.fromUri(NEXT_PAGE).build(),
                    previous: UriBuilder.fromUri(PREVIOUS_PAGE).build()
            ],
            [
                    first: UriBuilder.fromUri(FIRST_PAGE).build(),
                    last: UriBuilder.fromUri(LAST_PAGE).build(),
                    next: UriBuilder.fromUri(NEXT_PAGE).build()
            ],
            [
                    first: UriBuilder.fromUri(FIRST_PAGE).build(),
                    last: UriBuilder.fromUri(LAST_PAGE).build(),
                    previous: UriBuilder.fromUri(PREVIOUS_PAGE).build()
            ],
            [
                    first: UriBuilder.fromUri(FIRST_PAGE).build(),
                    last: UriBuilder.fromUri(LAST_PAGE).build(),
            ]
    ]
    static final List<String> BODY_LINKS_AS_JSON_LIST = [
            /"first": "$FIRST_PAGE",
            "last": "$LAST_PAGE",
            "next": "$NEXT_PAGE",
            "previous": "$PREVIOUS_PAGE"/,

            /"first": "$FIRST_PAGE",
            "last": "$LAST_PAGE",
            "next": "$NEXT_PAGE"/,

            /"first": "$FIRST_PAGE",
            "last": "$LAST_PAGE",
            "previous": "$PREVIOUS_PAGE"/,

            /"first": "$FIRST_PAGE",
            "last": "$LAST_PAGE"/
    ]

    Set<LogicalMetric> testLogicalMetrics
    Response response
    DateTime dateTime = new DateTime(1000L * 60 * 60 * 24 * 365 * 45)
    String formattedDateTime
    DataApiRequest apiRequest = Mock(DataApiRequest)
    ResultSet resultSet
    ByteArrayOutputStream os = new ByteArrayOutputStream()
    DateTimeZone originalTimeZone = DateTimeZone.getDefault()
    Pagination pagination
    Map<MetricColumn, Object> metricColumnsMap
    Set<MetricColumn> defaultRequestedMetrics
    SimplifiedIntervalList volatileIntervals = NO_INTERVALS;

    GString defaultJsonFormat = """{
                                    "rows" : [ {
                                                    "platform|desc" : "mobile \\" desc..",
                                                    "product|id" : "ymail",
                                                    "pageViews" : 10,
                                                    "dateTime" : "${-> formattedDateTime}",
                                                    "platform|id" : "mob",
                                                    "timeSpent" : 10,
                                                    "product|desc" : "yahoo, mail",
                                                    "property|desc": "United States"
                                                }, {
                                                    "platform|desc" : "desktop ,\\" desc..",
                                                    "product|id" : "ysports",
                                                    "pageViews" : 10,
                                                    "dateTime" : "${-> formattedDateTime}",
                                                    "platform|id" : "desk",
                                                    "timeSpent" : 10,
                                                    "product|desc" : "yahoo sports",
                                                    "property|desc": "India"
                                                  }
                                            ]
                                }"""

    GString defaultJsonApiFormat = """{
                                    "rows" : [ {
                                                    "product" : "ymail",
                                                    "pageViews" : 10,
                                                    "dateTime" : "${-> formattedDateTime}",
                                                    "platform" : "mob",
                                                    "property" : "US",
                                                    "timeSpent" : 10
                                                }, {
                                                    "product" : "ysports",
                                                    "pageViews" : 10,
                                                    "dateTime" : "${-> formattedDateTime}",
                                                    "platform" : "desk",
                                                    "property" : "IN",
                                                    "timeSpent" : 10
                                                  }
                                            ],
                                    "platform" : [
                                                {"id": "mob", "desc": "mobile \\" desc.."},
                                                {"id": "desk", "desc": "desktop ,\\" desc.."}
                                            ],
                                    "product" : [
                                                {"id": "ymail", "desc": "yahoo, mail"},
                                                {"id": "ysports", "desc": "yahoo sports"}
                                            ],
                                    "property" : [
                                                 {"id": "US", "desc": "United States"},
                                                 {"id": "IN", "desc": "India"}
                                            ]
                                }"""


    def setup() {
        DateTimeZone.setDefault(DateTimeZone.UTC)

        defaultRequestedMetrics = ["pageViews", "timeSpent"].collect {
            new MetricColumn(it)
        }
        metricColumnsMap = (defaultRequestedMetrics + [new MetricColumn("unrequestedMetric")]).collectEntries {
            [(it): new BigDecimal(10)]
        }
        response = new Response(
                buildTestResultSet(metricColumnsMap, defaultRequestedMetrics),
                apiRequest,
                NO_INTERVALS,
                volatileIntervals,
                [:],
                (Pagination) null,
                MAPPERS
        )
    }

    def cleanup() {
        DateTimeZone.setDefault(originalTimeZone)
    }

    ResultSet buildTestResultSet(Map<MetricColumn, Object> metricValues, Set<MetricColumn> requestedMetrics) {
        // Setup logical metrics for the API request mock
        testLogicalMetrics = requestedMetrics.collect {
            new LogicalMetric(null, null, it.name)
        } as Set

        apiRequest.getLogicalMetrics() >> { return testLogicalMetrics }

        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()

        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        List<DimensionColumn> dimensionColumns = [] as List
        Dimension dim1 = new KeyValueStoreDimension(
                "product",
                "product",
                dimensionFields,
                MapStoreManager.getInstance("product"),
                ScanSearchProviderManager.getInstance("product")
        )
        dim1.setLastUpdated(null)
        dimensionColumns << new DimensionColumn(dim1, dim1.getApiName())

        Dimension dim2 = new KeyValueStoreDimension(
                "platform",
                "platform",
                dimensionFields,
                MapStoreManager.getInstance("platform"),
                ScanSearchProviderManager.getInstance("platform")
        )
        dim2.setLastUpdated(null)
        dimensionColumns << new DimensionColumn(dim2, dim2.getApiName())

        Dimension dim3 = new KeyValueStoreDimension(
                "property",
                "property",
                dimensionFields,
                MapStoreManager.getInstance("property"),
                ScanSearchProviderManager.getInstance("property")
        )
        dim3.setLastUpdated(null)
        dimensionColumns << new DimensionColumn(dim3, dim3.getApiName())

        def schema = Mock(Schema)
        schema.getColumns(_) >> { Class cls ->
            if (cls == MetricColumn.class) {
                return metricColumnsMap.keySet() as LinkedHashSet
            }
            return dimensionColumns as LinkedHashSet
        }

        // Map same dimensionColumns as different DimensionRows

        Map<DimensionColumn, DimensionRow> dimensionRows1 = [
                (dimensionColumns[0]): BardDimensionField.makeDimensionRow(dimensionColumns[0].dimension, "ymail", "yahoo, mail"),
                (dimensionColumns[1]): BardDimensionField.makeDimensionRow(dimensionColumns[1].dimension, "mob", """mobile " desc.."""),
                (dimensionColumns[2]): BardDimensionField.makeDimensionRow(dimensionColumns[2].dimension, "US", "United States")
        ] as LinkedHashMap

        Map<DimensionColumn, DimensionRow> dimensionRows2 = [
                (dimensionColumns[0]): BardDimensionField.makeDimensionRow(dimensionColumns[0].dimension, "ysports", "yahoo sports"),
                (dimensionColumns[1]): BardDimensionField.makeDimensionRow(dimensionColumns[1].dimension, "desk", """desktop ," desc.."""),
                (dimensionColumns[2]): BardDimensionField.makeDimensionRow(dimensionColumns[2].dimension, "IN", "India")
        ] as LinkedHashMap

        Map<Dimension, Set<DimensionField>> defaultDimensionFieldsToShow = [
                (dim1): dimensionFields,
                (dim2): dimensionFields,
                (dim3): [BardDimensionField.DESC] as Set // to show only description
        ]

        apiRequest.getDimensionFields() >> { return defaultDimensionFieldsToShow }

        Result result1 = new Result(dimensionRows1, metricValues, dateTime)
        Result result2 = new Result(dimensionRows2, metricValues, dateTime)

        resultSet = new ResultSet([result1, result2], schema)

        //response without pagination
        response = new Response(resultSet, apiRequest, NO_INTERVALS, volatileIntervals,  [:], (Pagination) null, MAPPERS)

        pagination = Stub(Pagination) {
            getFirstPage() >> 1
            getLastPage() >> 3
            getNextPage() >> 3
            getPreviousPage() >> 1
            getPage() >> PAGE
            getNumResults() >> 6
            getPerPage() >> PER_PAGE
        }

        resultSet
    }

    @Unroll
    def "test for requested numeric metrics in the API response with the #linkNames links"() {
        setup:
        formattedDateTime = dateTime.toString(getDefaultFormat())

        Response paginatedResponse = new Response(
                resultSet,
                apiRequest,
                NO_INTERVALS,
                volatileIntervals,
                bodyLinks,
                pagination,
                MAPPERS
        )
        GString metaBlock = """{
                        "pagination": {
                            $bodyLinksAsJson,
                            "currentPage": $PAGE,
                            "rowsPerPage": $PER_PAGE,
                            "numberOfResults": 6
                        }
                    }"""
        String expectedJson = withMetaObject(defaultJsonFormat, metaBlock)

        ByteArrayOutputStream os = new ByteArrayOutputStream()
        paginatedResponse.write(os)

        expect:
        GroovyTestUtils.compareJson(os.toString(), expectedJson)

        where:
        linkNames << LINK_NAMES_LIST
        bodyLinks << BODY_LINKS_LIST
        bodyLinksAsJson << BODY_LINKS_AS_JSON_LIST
    }

    @Unroll
    def "Response properly serializes #type metrics in JSON format"() {
        given:
        formattedDateTime = dateTime.toString(getDefaultFormat())

        and: "The expected response, containing metric fields as the type under test"
        Map json = new JsonSlurper(JsonSortStrategy.SORT_NONE).parseText(defaultJsonFormat)
        json["rows"].each {
            it["luckyNumbers"] = firstValue
            it["unluckyNumbers"] = secondValue
        }
        String expectedJson = MAPPERS.getMapper().writeValueAsString(json)

        and: "A test result set with a few complex metrics"
        metricColumnsMap.put(new MetricColumn("luckyNumbers"), firstValue)
        metricColumnsMap.put(new MetricColumn("unluckyNumbers"), secondValue)
        defaultRequestedMetrics.addAll([new MetricColumn("luckyNumbers"), new MetricColumn("unluckyNumbers")])
        ResultSet resultSetWithComplexMetrics = buildTestResultSet(metricColumnsMap, defaultRequestedMetrics)

        when: "We serialize the response"
        Response complexResponse = new Response(
                resultSetWithComplexMetrics,
                apiRequest,
                NO_INTERVALS,
                NO_INTERVALS,
                [:],
                (Pagination) null,
                MAPPERS
        )
        ByteArrayOutputStream os = new ByteArrayOutputStream()
        complexResponse.write(os)

        then: "The response is serialized correctly"
        GroovyTestUtils.compareJson(os.toString(), expectedJson)

        where:
        type      | firstValue                           | secondValue
        "String"  | "1, 3, 7"                            | "2"
        "boolean" | true                                 | false
        "JsonNode"| '{"values": "1, 3, 7", "length": 3}' | '{"values": "2", "length": 1}'
        "null"    | null                                 | null
    }

    @Unroll
    def "JSONApi response is correct for a known result set with link names #linkNames"() {
        setup:
        apiRequest.getFormat() >> ResponseFormatType.JSONAPI
        formattedDateTime = dateTime.toString(getDefaultFormat())
        GString metaBlock = """{
                        "pagination": {
                            $bodyLinksAsJson,
                            "currentPage": $PAGE,
                            "rowsPerPage": $PER_PAGE,
                            "numberOfResults": 6
                        }
                    }"""

        response = new Response(resultSet, apiRequest, NO_INTERVALS, volatileIntervals, bodyLinks, pagination, MAPPERS)

        String expectedJson = withMetaObject(defaultJsonApiFormat, metaBlock)

        ByteArrayOutputStream os = new ByteArrayOutputStream()
        response.write(os)

        expect:
        GroovyTestUtils.compareJson(os.toString(), expectedJson, JsonSortStrategy.SORT_BOTH)

        where:
        linkNames << LINK_NAMES_LIST
        bodyLinks << BODY_LINKS_LIST
        bodyLinksAsJson << BODY_LINKS_AS_JSON_LIST
    }

    def "test CSV response header ordering"() {

        List dimensionNames = [
                "platform",
                "platform_version",
                "product",
                "product_family",
                "pty_country",
                "user_state"
        ]

        Map<Dimension, Set<DimensionField>> defaultDimensionFieldsToShow = [:] as LinkedHashMap

        DataApiRequest apiRequest1 = Mock(DataApiRequest) {
            getLogicalMetrics() >>  testLogicalMetrics
        }

        // for now all dimensions contain only two fields ID and Description
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        // Longer list for ordering test
        List<DimensionColumn> dimensionColumns = [] as List
        Dimension dimension
        for (String it: dimensionNames) {

            dimension = new KeyValueStoreDimension(
                    it,
                    it,
                    dimensionFields,
                    MapStoreManager.getInstance(it),
                    ScanSearchProviderManager.getInstance(it)
            )

            if (it == "pty_country") {
                // to show only desc for pty_country
                defaultDimensionFieldsToShow.put(dimension, [BardDimensionField.DESC] as Set)
            } else {
                defaultDimensionFieldsToShow.put(dimension, dimensionFields)
            }

            dimension.setLastUpdated(null)
            dimensionColumns << DimensionColumn.addNewDimensionColumn(new PhysicalTable("", DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC), [(dimension.getApiName()): dimension.getApiName()]), dimension)
        }

        apiRequest1.getDimensionFields() >> defaultDimensionFieldsToShow

        Map<DimensionColumn, DimensionRow> dimensionRows = [
                (dimensionColumns[0]): BardDimensionField.makeDimensionRow(dimensionColumns[0].dimension, "platform", "yahoo, mail"),
                (dimensionColumns[1]): BardDimensionField.makeDimensionRow(dimensionColumns[1].dimension, "platform_version", "yahoo, mail"),
                (dimensionColumns[2]): BardDimensionField.makeDimensionRow(dimensionColumns[2].dimension, "product", "yahoo, mail"),
                (dimensionColumns[3]): BardDimensionField.makeDimensionRow(dimensionColumns[3].dimension, "product_family", "yahoo, mail"),
                (dimensionColumns[4]): BardDimensionField.makeDimensionRow(dimensionColumns[4].dimension, "pty_country", "yahoo, mail"),
                (dimensionColumns[5]): BardDimensionField.makeDimensionRow(dimensionColumns[5].dimension, "user_state", "yahoo, mail")
        ] as LinkedHashMap

        // Setup metric columns which are returned by the schema.getColumns method
        def metricColumnNames = ["pageViews", "timeSpent", "unrequestedMetric"]
        Set<MetricColumn> metricColumns = metricColumnNames.collect {
            new MetricColumn(it)
        } as LinkedHashSet

        //Setup metricValues
        Map<MetricColumn, BigDecimal> metricValues = metricColumns.collectEntries {
            [(it): 10]
        }
        def schema = Mock(Schema)
        schema.getColumns(_) >> { Class cls ->
            if (cls == MetricColumn.class) {
                return metricColumns
            }
            return dimensionColumns as LinkedHashSet
        }

        def expected = ["dateTime"]
        dimensionNames.each {
            if (it == "pty_country") {
                expected.add(it + "|desc")
            } else {
                expected.add(it + "|id")
                expected.add(it + "|desc")
            }
        }
        testLogicalMetrics.each { expected.add(it.getName()) }


        Result result = new Result(dimensionRows, metricValues, dateTime)
        ResultSet resultSet = new ResultSet([result, result], schema)

        Response response = new Response(
                resultSet,
                apiRequest1,
                NO_INTERVALS,
                volatileIntervals,
                [:],
                (Pagination) null,
                MAPPERS
        )
        String dateStr = dateTime.toString("YYYY-MM-dd HH:mm:ss.SSS")

        CsvSchema csvSchema = response.buildCsvHeaders()
        def actualList = MAPPERS.getCsvMapper()
                .writer()
                .with(csvSchema.withSkipFirstDataRow(true))
                .writeValueAsString(Collections.emptyMap())
                .replaceAll("\\r?\\n?", "")
                .split(',') as List

        expect:
        actualList == expected
    }

    def "test CSV response"() {
        setup:
        formattedDateTime = dateTime.toString(getDefaultFormat())

        response.writeCsvResponse(os)

        String expectedCSV =
                """dateTime,product|id,product|desc,platform|id,platform|desc,property|desc,pageViews,timeSpent
                |\"$formattedDateTime\",ymail,"yahoo, mail",mob,"mobile "" desc..","United States",10,10
                |\"$formattedDateTime\",ysports,"yahoo sports",desk,"desktop ,"" desc..",India,10,10
                |""".stripMargin()

        String csvResponse = os.toString()

        expect:
        csvResponse == expectedCSV
    }

    @Unroll
    def "test for existence of missing intervals in response when #arePaginating"() {
        setup:
        boolean partialDataSetting = PARTIAL_DATA.isOn()
        PARTIAL_DATA.setOn(true)

        formattedDateTime = dateTime.toString(getDefaultFormat())
        GString paginationBlock = """, "pagination": {
                        $bodyLinksAsJson,
                        "currentPage": $PAGE,
                        "rowsPerPage": $PER_PAGE,
                        "numberOfResults": 6
                    }"""
        GString metaBlock = """{
                "missingIntervals" : [
                    "2014-07-01 00:00:00.000/2014-07-08 00:00:00.000",
                    "2014-07-15 00:00:00.000/2014-07-22 00:00:00.000"
                 ]
                 ${paginating ? paginationBlock : ""}
                  }
                }"""

        SimplifiedIntervalList missingIntervals = [new Interval("2014-07-01/2014-07-08"), new Interval(
                "2014-07-15/2014-07-22"
        )] as SimplifiedIntervalList
        Response response1 = new Response(
                resultSet,
                apiRequest,
                missingIntervals,
                volatileIntervals,
                bodyLinks,
                paginating ? pagination : null,
                MAPPERS
        )

        String expectedJson = withMetaObject(defaultJsonFormat, metaBlock)
        response1.write(os)

        expect:
        GroovyTestUtils.compareJson(os.toString(), expectedJson)

        cleanup:
        PARTIAL_DATA.setOn(partialDataSetting)

        where:
        paginating << [false] + [true] * LINK_NAMES_LIST.size()
        linkNames << [''] + LINK_NAMES_LIST
        bodyLinks << [[:] as Map] + BODY_LINKS_LIST
        bodyLinksAsJson << [''] + BODY_LINKS_AS_JSON_LIST
        arePaginating = paginating ? "paginating with pagination links $linkNames" : "not paginating"
    }

    String getDefaultFormat() {
        return "YYYY-MM-dd HH:mm:ss.SSS"
    }

    @Unroll
    def "test json format with format #outputDateTimeFormat"() {
        setup:
        formattedDateTime = dateTime.toString(outputDateTimeFormat)
        DateTimeFormatter originalFormat = DateTimeFormatterFactory.DATETIME_OUTPUT_FORMATTER
        DateTimeFormatterFactory.DATETIME_OUTPUT_FORMATTER = null
        systemConfig.setProperty(
                DateTimeFormatterFactory.OUTPUT_DATETIME_FORMAT,
                outputDateTimeFormat
        )
        response = new Response(
                buildTestResultSet(metricColumnsMap, defaultRequestedMetrics),
                apiRequest,
                NO_INTERVALS,
                volatileIntervals,
                [:],
                (Pagination) null,
                MAPPERS
        )

        response.write(os)

        expect:
        GroovyTestUtils.compareJson(os.toString(), defaultJsonFormat)

        cleanup:
        systemConfig.clearProperty(DateTimeFormatterFactory.OUTPUT_DATETIME_FORMAT)
        DateTimeFormatterFactory.DATETIME_OUTPUT_FORMATTER = originalFormat

        where:
        outputDateTimeFormat << [
            "YYYY-MM-dd HH:mm:ss.SSSZZZ",
            "YYYY-MM-dd HH:mm:ss.SSSZ",
            "YYYY-MM-dd HH:mm:ss.SSS",
            "YYYY-MM-dd HH:mm",
            "YYYY-MM-dd"
        ]
    }

    def "Test dimension column name builds correctly"() {
        setup: "Build a fake dimension and dimension field"
        Dimension dimension = Mock(Dimension)
        DimensionField dimensionField = Mock(DimensionField)

        and: "With values to be used to build the string"
        dimension.getApiName() >> "Foo"
        dimensionField.getName() >> "Bar"

        expect: "Verify that the correct string is built"
        Response.getDimensionColumnName(dimension, dimensionField) == "Foo|Bar"
    }

    def "Test getDimensionColumnName updates cache"() {
        setup: "Build a fake dimension and dimension field"
        Dimension dimension = Mock(Dimension)
        DimensionField dimensionField = Mock(DimensionField)

        and: "With values to be used to build the string"
        dimension.getApiName() >> "Foo"
        dimensionField.getName() >> "Bar"

        and: "Remove the fake dimension from the cache, if it exists"
        Response.DIMENSION_FIELD_COLUMN_NAMES.remove(dimension)

        when: "Build, cache and retrieve the name"
        String name = Response.getDimensionColumnName(dimension, dimensionField)

        then: "Verify that the name is correct and that the name is in the cache"
        name == "Foo|Bar"
        Response.DIMENSION_FIELD_COLUMN_NAMES.get(dimension).get(dimensionField) == name
    }

    def "Test getDimensionColumnName retrieves from cache"() {
        setup: "Build a fake dimension and dimension field"
        Dimension dimension = Mock(Dimension)
        DimensionField dimensionField = Mock(DimensionField)

        and: "With values that should never be used"
        0 * dimension.getApiName()
        0 * dimensionField.getName()

        and: "Ensure that the expected value is in the cache"
        Response.DIMENSION_FIELD_COLUMN_NAMES.put(dimension, [(dimensionField): "Foo|Bar"])

        expect: "The correct value wil be retrieved without invoking methods on the mock"
        Response.getDimensionColumnName(dimension, dimensionField) == "Foo|Bar"
    }

    /**
     * Given a JSON string, and a JSON meta object as a  string, combines the two into a new JSON string.
     * <p>
     * The meta block is added at the top level of the new JSON string.
     *
     * @param jsonString  A String representing the base JSON string.
     * @param metaBlock  The meta block to be merged with the JSON string.
     * @return A new JSON String that merges the jsonString with the metaBlock.
     */
    String withMetaObject(GString jsonString, GString metaBlock) {
        JsonSlurper jsonSlurper = new JsonSlurper(JsonSortStrategy.SORT_NONE)
        Map baseJson = jsonSlurper.parseText(jsonString)
        def metaJson = jsonSlurper.parseText(metaBlock)
        baseJson.put("meta", metaJson)
        MAPPERS.getMapper().writeValueAsString(baseJson)
    }
}
