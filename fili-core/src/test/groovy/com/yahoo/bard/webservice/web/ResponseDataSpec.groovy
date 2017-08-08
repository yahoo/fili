// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

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
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory
import com.yahoo.bard.webservice.util.Pagination
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification

import java.util.stream.Stream

class ResponseDataSpec extends Specification {

    SystemConfig systemConfig = SystemConfigProvider.getInstance()

    static final int PAGE = 2
    static final int PER_PAGE = 2

    Set<Column> columns
    Set<LogicalMetric> testLogicalMetrics
    ResponseData response
    DateTime dateTime = new DateTime(1000L * 60 * 60 * 24 * 365 * 45)
    DataApiRequest apiRequest = Mock(DataApiRequest)
    ResultSet resultSet
    ByteArrayOutputStream os = new ByteArrayOutputStream()
    DateTimeZone originalTimeZone = DateTimeZone.getDefault()
    Pagination pagination
    Map<MetricColumn, Object> metricColumnsMap
    Set<MetricColumn> defaultRequestedMetrics
    SimplifiedIntervalList volatileIntervals = new SimplifiedIntervalList();

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

    def "Test dimension column name builds correctly"() {
        setup: "Build a fake dimension and dimension field"
        Dimension dimension = Mock(Dimension)
        DimensionField dimensionField = Mock(DimensionField)

        and: "With values to be used to build the string"
        dimension.getApiName() >> "Foo"
        dimensionField.getName() >> "Bar"

        expect: "Verify that the correct string is built"
        ResponseData.getDimensionColumnName(dimension, dimensionField) == "Foo|Bar"
    }

    def "test for generate api metrics column"() {
        setup:
        Set<String> apiColumns = new HashSet<>();
        apiColumns.add("pageViews")
        apiColumns.add("timeSpent")
        Set<MetricColumn> responseColumns = response.generateApiMetricColumns(apiColumns)
        Set<MetricColumn> expectedColumns = ["pageViews", "timeSpent"].collect {
            new MetricColumn(it)
        }
        expect:
        responseColumns == expectedColumns
    }

    def "test for generate dimension column header"() {
        setup:
        Map dimensionFields = response.getRequestedApiDimensionFields()
        Map.Entry firstEntry = ++dimensionFields.entrySet().iterator();
        Stream responseColumns = response.generateDimensionColumnHeaders(firstEntry)

        expect:
        responseColumns.iterator().next() == "product|id"
    }

    def "test for build result row"() {
        setup:
        Map row = response.buildResultRow(resultSet.iterator().next())
        Map<String, Object> expectedRow = new LinkedHashMap<>();
        expectedRow.put("dateTime",dateTime.toString(DateTimeFormatterFactory.getOutputFormatter()))
        expectedRow.put("product|id","ymail")
        expectedRow.put("product|desc","yahoo, mail")
        expectedRow.put("platform|id","mob")
        expectedRow.put("platform|desc","mobile \" desc..")
        expectedRow.put("property|desc","United States")
        expectedRow.put("pageViews",10)
        expectedRow.put("timeSpent",10)

        expect:
        row == expectedRow
    }
}
