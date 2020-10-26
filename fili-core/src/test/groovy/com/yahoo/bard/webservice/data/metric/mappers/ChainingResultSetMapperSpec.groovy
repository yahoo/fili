// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.metric.mappers

import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.web.ChainingResultSetMapper
import org.joda.time.DateTime
import spock.lang.Specification
import spock.lang.Unroll

class ChainingResultSetMapperSpec extends Specification {

    MetricColumn column = new MetricColumn("metric column")


    SketchRoundUpMapper sketchRoundUpMapper = new SketchRoundUpMapper(column.name)
    NoOpResultSetMapper noOpResultSetMapper = new NoOpResultSetMapper()
    List<ResultSetMapper> chainedMappers = Arrays.asList(sketchRoundUpMapper,noOpResultSetMapper)
    ChainingResultSetMapper chainingResultSetMapper = new ChainingResultSetMapper(chainedMappers)

    @Unroll
    def "The SketchRoundUpMapper in chained list exist and did its work"() {
        given: "A result containing the floating point value"

        ResultSetSchema schema = new ResultSetSchema(DefaultTimeGrain.DAY, [column].toSet())

        Result result = new Result(
                [:],
                [(column): 1.5 as BigDecimal] as Map<MetricColumn, Object>,
                new DateTime()
        )

        ResultSet resultSet = new ResultSet(schema, [result] as List)

        expect:
        chainingResultSetMapper.map(resultSet).get(0).getMetricValueAsNumber(column) == 2 as BigDecimal
    }

    @Unroll
    def "Same result is returned when there are no mappers in chain"() {
        ResultSetSchema schema = new ResultSetSchema(DefaultTimeGrain.DAY, [column].toSet())

        Result result = new Result(
                [:],
                [(column): 1.5 as BigDecimal] as Map<MetricColumn, Object>,
                new DateTime()
        )

        ResultSet resultSet = new ResultSet(schema, [result] as List)
        ChainingResultSetMapper chainingResultSetMapper = new ChainingResultSetMapper([] as List)

        expect:
        chainingResultSetMapper.map(resultSet) == resultSet

    }

    @Unroll
    def "ChainingResultSetMapper overridden map methods were never called as they are NoOP"() {
        given: "A result containing the floating point value"

        ResultSetSchema schema = new ResultSetSchema(DefaultTimeGrain.DAY, [column].toSet())

        Result result = new Result(
                [:],
                [(column): 1.5 as BigDecimal] as Map<MetricColumn, Object>,
                new DateTime()
        )

        ResultSet resultSet = new ResultSet(schema,[result] as List)

        when:
        chainingResultSetMapper.map(resultSet)

        then:
        0 * chainingResultSetMapper.map(schema)
        0 * chainingResultSetMapper.map(result, schema)
    }
}
