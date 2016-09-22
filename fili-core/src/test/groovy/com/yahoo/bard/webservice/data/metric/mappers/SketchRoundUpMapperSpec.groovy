// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers

import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.table.Schema

import org.joda.time.DateTime

import spock.lang.Specification
import spock.lang.Unroll

class SketchRoundUpMapperSpec extends Specification {

    Schema schema = new Schema(DefaultTimeGrain.DAY)
    MetricColumn column = MetricColumn.addNewMetricColumn(schema, "Row row row your boat")
    SketchRoundUpMapper mapper = new SketchRoundUpMapper(column.name)

    @Unroll
    def "The mapper rounds #floatingPoint to #integer"() {
        given: "A result containing the floating point value"
        Result result = new Result([:], [(column): floatingPoint as BigDecimal], new DateTime())

        expect: "The sketch round up mapper returns a new result with the rounded value"
        mapper.map(result, schema).getMetricValueAsNumber(column) == integer as BigDecimal

        where:
        floatingPoint | integer
        0.0           | 0
        1.0           | 1
        1.5           | 2
        1.6           | 2
        1.3           | 2
        -1.5          | -1
        -1.6          | -1
        -1.3          | -1
    }

    def "The mapper passes along results with a null value unmodified"() {
        given: "A result containing a null value"
        Result result = new Result([:], [(column): null], new DateTime())

        expect:
        mapper.map(result, schema) == result
    }
}
