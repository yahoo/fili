// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

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
import com.yahoo.bard.webservice.util.Pagination
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.fasterxml.jackson.dataformat.csv.CsvSchema

class CsvResponseWriterSpec extends ResponseWriterSpec {

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

        DataApiRequest apiRequest = Mock(DataApiRequest) {
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
            dimensionColumns << new DimensionColumn(dimension)
        }

        apiRequest.getDimensionFields() >> defaultDimensionFieldsToShow

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
        ResultSetSchema schema = Mock(ResultSetSchema)
        schema.getColumns() >> columns

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
        ResultSet resultSet = new ResultSet(schema, [result, result])

        ResponseData response = new ResponseData(
                resultSet,
                apiRequest,
                new SimplifiedIntervalList(),
                volatileIntervals,
                (Pagination) null,
                [:]
        )

        csvResponseWriter = new CsvResponseWriter(MAPPERS)
        CsvSchema csvSchema = csvResponseWriter.buildCsvHeaders(response)
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
        csvResponseWriter = new CsvResponseWriter(MAPPERS)
        csvResponseWriter.write(apiRequest, response, os)

        String expectedCSV =
                """dateTime,product|id,product|desc,platform|id,platform|desc,property|desc,pageViews,timeSpent
                |\"$formattedDateTime\",ymail,"yahoo, mail",mob,"mobile "" desc..","United States",10,10
                |\"$formattedDateTime\",ysports,"yahoo sports",desk,"desktop ,"" desc..",India,10,10
                |""".stripMargin()

        String csvResponse = os.toString()

        expect:
        csvResponse == expectedCSV
    }
}
