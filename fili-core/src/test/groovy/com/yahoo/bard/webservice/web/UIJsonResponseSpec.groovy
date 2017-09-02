// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
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
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.Pagination
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification

class UIJsonResponseSpec extends Specification {
    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    ResultSetSchema newSchema
    Map<DimensionColumn, DimensionRow> dimensionRows
    Map<MetricColumn, BigDecimal> metricValues
    DateTime timeStamp
    LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> defaultDimensionFieldsToShow
    SimplifiedIntervalList volatileIntervals = []
    FiliResponseWriter filiResponseWriter

    def setup() {
        // Default JodaTime zone to UTC
        DateTimeZone.setDefault(DateTimeZone.UTC)

        // Build a default timestamp
        timeStamp = new DateTime(10000)


        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        // Add a dimension and some metrics to the schema
        Dimension newDimension = new KeyValueStoreDimension(
                "gender",
                "gender-description",
                dimensionFields,
                MapStoreManager.getInstance("gender"),
                ScanSearchProviderManager.getInstance("gender"),
                [] as Set
        )
        newDimension.setLastUpdated(timeStamp)
        DimensionColumn dimensionColumn = new DimensionColumn(newDimension)
        MetricColumn metricColumn1 = new MetricColumn("metricColumn1Name")
        MetricColumn metricColumn2 = new MetricColumn("metricColumn2Name")

        // Build a default dimension row
        DimensionRow dimensionRow = BardDimensionField.makeDimensionRow(
                newDimension,
                "gender-one-id",
                "gender-one-desc"
        )
        dimensionRows = [(dimensionColumn): dimensionRow]

        // Build some default dimension values
        metricValues = [
                (metricColumn1): 1234567.1234,
                (metricColumn2): 1234567.1234
        ]

        defaultDimensionFieldsToShow = [
                (newDimension): dimensionFields
        ]

        // Build a default schema
        newSchema = new ResultSetSchema(DAY, [dimensionColumn, metricColumn1, metricColumn2] as Set)

        filiResponseWriter = new FiliResponseWriter(new FiliResponseWriterSelector(
                new CsvResponseWriter(MAPPERS),
                new JsonResponseWriter(MAPPERS),
                new JsonApiResponseWriter(MAPPERS)
        ))
    }

    def "Get single row response"() {

        given: "A Result Set with one row"
        Result r1 = new Result(dimensionRows, metricValues, timeStamp)
        ResultSet resultSet = new ResultSet(newSchema, [r1])

        and: "An API Request"
        LinkedHashSet<String> apiMetricColumnNames = getApiMetricColumnNames()


        and: "An expected json serialization"
        DataApiRequest apiRequest = Mock(DataApiRequest)
        apiRequest.getFormat()  >> ResponseFormatType.JSON
        String expectedJSON = """{
            "rows":[{
                        "metricColumn1Name":1234567.1234,
                        "dateTime":"1970-01-01 00:00:10.000",
                        "gender|desc":"gender-one-desc",
                        "metricColumn2Name":1234567.1234,
                        "gender|id":"gender-one-id"
            }]
        }"""

        when: "get and serialize a JsonResponse"
        ResponseData jro = new ResponseData(
                resultSet,
                apiMetricColumnNames,
                defaultDimensionFieldsToShow,
                new SimplifiedIntervalList(),
                volatileIntervals,
                (Pagination) null,
                [:]
        )

        ByteArrayOutputStream os = new ByteArrayOutputStream()
        filiResponseWriter.write(apiRequest, jro, os)

        String responseJSON = os.toString()

        then: "The serialized JsonResponse matches what we expect"
        GroovyTestUtils.compareJson(responseJSON, expectedJSON)
    }

    def "Get multiple rows response"() {

        given: "A Result Set with multiple rows"
        Result r1 = new Result(dimensionRows, metricValues, timeStamp)
        ResultSet resultSet = new ResultSet(newSchema, [r1, r1, r1])

        and: "An API Request"
        DataApiRequest apiRequest = Mock(DataApiRequest)
        apiRequest.getFormat()  >> ResponseFormatType.JSON
        LinkedHashSet<String> apiMetricColumnNames = getApiMetricColumnNames()

        apiRequest.getDimensionFields() >> defaultDimensionFieldsToShow

        and: "An expected json serialization"
        String expectedJSON = """{
            "rows":[
                {
                    "metricColumn1Name":1234567.1234,
                    "dateTime":"1970-01-01 00:00:10.000",
                    "gender|desc":"gender-one-desc",
                    "metricColumn2Name":1234567.1234,
                    "gender|id":"gender-one-id"
                },
                {
                    "metricColumn1Name":1234567.1234,
                    "dateTime":"1970-01-01 00:00:10.000",
                    "gender|desc":"gender-one-desc",
                    "metricColumn2Name":1234567.1234,
                    "gender|id":"gender-one-id"
                },
                {
                    "metricColumn1Name":1234567.1234,
                    "dateTime":"1970-01-01 00:00:10.000",
                    "gender|desc":"gender-one-desc",
                    "metricColumn2Name":1234567.1234,
                    "gender|id":"gender-one-id"
                }
            ]
        }"""

        when: "We get and serialize a JsonResponse for it"
        ResponseData jro = new ResponseData(
                resultSet,
                apiMetricColumnNames,
                defaultDimensionFieldsToShow,
                new SimplifiedIntervalList(),
                volatileIntervals,
                (Pagination) null,
                [:]
        )
        ByteArrayOutputStream os = new ByteArrayOutputStream()
        filiResponseWriter.write(apiRequest, jro, os)

        String responseJSON = os.toString()
        then: "The serialized JsonResponse matches what we expect"
        GroovyTestUtils.compareJson(responseJSON, expectedJSON)
    }

    Set<String> getApiMetricColumnNames() {
        return newSchema.getColumns(MetricColumn).collect {it.name}
    }
}
