// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers

import com.yahoo.bard.webservice.table.PhysicalTable
import org.joda.time.DateTimeZone

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.Schema

import org.joda.time.DateTime

import spock.lang.Specification

class RowNumMapperSpec extends Specification {

    def "check RowNumMapper"(){

        DateTime dateTime = DateTime.now()

        /**
         * Create a dummy schema and add dummy metric columns to it
         */
        Schema schema = new Schema(DAY)
        MetricColumn mc1 = MetricColumn.addNewMetricColumn(schema, "m1")
        MetricColumn mc2 = MetricColumn.addNewMetricColumn(schema, "m2")

        // Initialize dummy metric values as a map of metric column and its value
        LinkedHashMap<MetricColumn, BigDecimal> mv1 = new LinkedHashMap<>()
        mv1.put(mc1, BigDecimal.valueOf(12345))
        mv1.put(mc2, BigDecimal.valueOf(67890))

        // for now all dimensions contain only two fields ID and Description
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        // Initialize dummy Dimension's which will be used to create DimensionColumn
        Dimension d1 = new KeyValueStoreDimension("d1", "d1-desc", dimensionFields, MapStoreManager.getInstance("d1"), ScanSearchProviderManager.getInstance("d1"))
        Dimension d2 = new KeyValueStoreDimension("d2", "d2-desc", dimensionFields, MapStoreManager.getInstance("d2"), ScanSearchProviderManager.getInstance("d2"))

        // Add dimension columns to the dummy schema created earlier
        DimensionColumn dc1 = DimensionColumn.addNewDimensionColumn(schema, d1, new PhysicalTable("abc", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        DimensionColumn dc2 = DimensionColumn.addNewDimensionColumn(schema, d2, new PhysicalTable("abc", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))

        // Create dummy DimensionRow's
        DimensionRow dr1 = BardDimensionField.makeDimensionRow(d1, "id1", "desc1")
        DimensionRow dr2 = BardDimensionField.makeDimensionRow(d2, "id2", "desc2")

        //Initialize dummy dimension row's as a map of DimensionRow and DimensionColumn
        LinkedHashMap<DimensionColumn, DimensionRow> drow = new LinkedHashMap<>()
        drow.put(dc1, dr1)
        drow.put(dc2, dr2)

        // Create a result using the Dimension Row's map and Metric Value's map
        Result r1 = new Result(drow, mv1, dateTime)

        LinkedHashMap<MetricColumn, BigDecimal> mv2 = new LinkedHashMap<>()
        mv2.put(mc1, BigDecimal.valueOf(1234))
        mv2.put(mc2, BigDecimal.valueOf(5678))

        Result r2 = new Result(drow, mv2, dateTime)

        // From the dummy result's  created above, create a ResultSet
        // This is the resultSet which we pass to the mapper
        ResultSet resultSet = new ResultSet(Arrays.asList(r1, r2), schema)

        // Create a dummy column with name rowNum
        MetricColumn rowNum = MetricColumn.addNewMetricColumn(schema, TestApiMetricName.A_ROW_NUM.getApiName())

        // Add the new column with respective values, which we expect the rowNumMapper would do
        LinkedHashMap<MetricColumn, BigDecimal> mappedMv1 = new LinkedHashMap<>()
        mappedMv1.put(mc1, BigDecimal.valueOf(12345))
        mappedMv1.put(mc2, BigDecimal.valueOf(67890))
        mappedMv1.put(rowNum, BigDecimal.valueOf(0))

        LinkedHashMap<MetricColumn, BigDecimal> mappedMv2 = new LinkedHashMap<>()
        mappedMv2.put(mc1, BigDecimal.valueOf(1234))
        mappedMv2.put(mc2, BigDecimal.valueOf(5678))
        mappedMv2.put(rowNum, BigDecimal.valueOf(1))

        Result mappedR1 = new Result(drow, mappedMv1, dateTime)
        Result mappedR2 = new Result(drow, mappedMv2, dateTime)

        ResultSetMapper rowNumMapper = new RowNumMapper()

        // Create an expected mapped ResultSet to be compared with
        ResultSet mappedResultSet = new ResultSet(Arrays.asList(mappedR1, mappedR2), schema)

        expect:
        mappedResultSet == rowNumMapper.map(resultSet)
    }
}
