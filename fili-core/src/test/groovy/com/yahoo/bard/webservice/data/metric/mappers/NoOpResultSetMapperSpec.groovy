// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
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

class NoOpResultSetMapperSpec extends Specification {

    def "check NoOpResultSetMapper"(){
        given:
        // for now all dimensions contain only two fields ID and Description
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        Map<DimensionColumn, DimensionRow> dimensionRows1 = ["dimName1", "dimName2"].collectEntries {
            DimensionColumn dc = new DimensionColumn(new KeyValueStoreDimension(it, it, dimensionFields, MapStoreManager.getInstance(it), ScanSearchProviderManager.getInstance(it)), it)
            DimensionRow dr = BardDimensionField.makeDimensionRow(dc.dimension, ""+it+"-id", ""+it+"-desc")
            [(dc) : dr]
        }

        Map<MetricColumn, BigDecimal> metricValues1 = ["metVal1", "metVal2"].collectEntries {
            [( new MetricColumn(it)) : 10.05 ]
        }


        Result rs1 = new Result(dimensionRows1, metricValues1, DateTime.now())

        Map<DimensionColumn, DimensionRow> dimensionRows2 = ["dimName3", "dimName4"].collectEntries {
            DimensionColumn dc = new DimensionColumn(new KeyValueStoreDimension(it, it, dimensionFields, MapStoreManager.getInstance(it), ScanSearchProviderManager.getInstance(it)), it)
            DimensionRow dr = BardDimensionField.makeDimensionRow(dc.dimension, ""+it+"-id", ""+it+"-desc")
            [(dc) : dr]
        }

        Map<MetricColumn, BigDecimal> metricValues2 = ["metVal1", "metVal2"].collectEntries {
            [( new MetricColumn(it)) : 10.05 ]
        }

        Result rs2 = new Result(dimensionRows2, metricValues2, DateTime.now())

        Schema schema = new Schema(DAY)

        ResultSet resultSet = new ResultSet([rs1, rs2], schema)

        ResultSetMapper resultSetMapper = new NoOpResultSetMapper()

        expect:
        //check for result set
        resultSet.equals(resultSetMapper.map(resultSet))
        //check for result
        rs1.equals(resultSetMapper.map(rs1, null))
        //check for schema
        schema.equals(resultSetMapper.map(schema))
    }
}
