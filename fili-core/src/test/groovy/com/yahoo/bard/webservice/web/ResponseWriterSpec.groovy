// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
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
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.util.Pagination
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification

import javax.ws.rs.core.UriBuilder

/**
 * This class host common fields and functions shared by all ResponseWriterSpec.
 */
abstract class ResponseWriterSpec extends Specification {

    static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

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

    Set<Column> columns
    Set<LogicalMetric> testLogicalMetrics
    ResponseData response
    CsvResponseWriter csvResponseWriter
    JsonResponseWriter jsonResponseWriter
    JsonApiResponseWriter jsonApiResponseWriter

    DateTime dateTime = new DateTime(1000L * 60 * 60 * 24 * 365 * 45)
    String formattedDateTime
    DataApiRequest apiRequest = Mock(DataApiRequest)
    ResultSet resultSet
    ByteArrayOutputStream os = new ByteArrayOutputStream()
    DateTimeZone originalTimeZone = DateTimeZone.getDefault()
    Pagination pagination
    Map<MetricColumn, Object> metricColumnsMap
    Set<MetricColumn> defaultRequestedMetrics
    SimplifiedIntervalList volatileIntervals = new SimplifiedIntervalList();

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
        response = new ResponseData(
                buildTestResultSet(metricColumnsMap, defaultRequestedMetrics),
                apiRequest,
                new SimplifiedIntervalList(),
                volatileIntervals,
                (Pagination) null,
                [:]
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
        dimensionColumns << new DimensionColumn(dim1)

        Dimension dim2 = new KeyValueStoreDimension(
                "platform",
                "platform",
                dimensionFields,
                MapStoreManager.getInstance("platform"),
                ScanSearchProviderManager.getInstance("platform")
        )
        dim2.setLastUpdated(null)
        dimensionColumns << new DimensionColumn(dim2)

        Dimension dim3 = new KeyValueStoreDimension(
                "property",
                "property",
                dimensionFields,
                MapStoreManager.getInstance("property"),
                ScanSearchProviderManager.getInstance("property")
        )
        dim3.setLastUpdated(null)
        dimensionColumns << new DimensionColumn(dim3)

        ResultSetSchema schema = Mock(ResultSetSchema)
        columns = [] as Set
        columns.addAll(metricColumnsMap.keySet())
        columns.addAll(dimensionColumns)

        schema.getColumns() >> columns
        schema.getColumns(_) >> { Class cls ->
            if (cls == MetricColumn.class) {
                return metricColumnsMap.keySet() as LinkedHashSet
            }
            return dimensionColumns as LinkedHashSet
        }
        schema.getGranularity() >> DAY

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

        resultSet = new ResultSet(schema, [result1, result2])

        //response without pagination
        response = new ResponseData(
                resultSet,
                apiRequest,
                new SimplifiedIntervalList(),
                volatileIntervals,
                (Pagination) null,
                [:]
        )

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

    public String getDefaultFormat() {
        return "YYYY-MM-dd HH:mm:ss.SSS"
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
        Map baseJson = (Map) jsonSlurper.parseText(jsonString)
        def metaJson = jsonSlurper.parseText(metaBlock)
        baseJson.put("meta", metaJson)
        MAPPERS.getMapper().writeValueAsString(baseJson)
    }
}
