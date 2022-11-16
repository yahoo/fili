// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.metric.mappers

import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
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
        ResultSet resultSet = new ResultSet(schema,[result] as List)

        expect: "SketchRoundupMapper was applied on ResultSet"
        chainingResultSetMapper.map(resultSet).get(0).getMetricValueAsNumber(column) == 2 as BigDecimal
    }

    @Unroll
    def "Same result is returned when there are no mappers in chain"() {
        given: "A empty list of chainingResultSetMapper"
        ChainingResultSetMapper chainingResultSetMapper = new ChainingResultSetMapper([] as List)

        ResultSetSchema schema = new ResultSetSchema(DefaultTimeGrain.DAY, [column].toSet())
        Result result = new Result(
                [:],
                [(column): 1.5 as BigDecimal] as Map<MetricColumn, Object>,
                new DateTime()
        )
        ResultSet resultSet = new ResultSet(schema,[result] as List)

        expect: "ResultSet doesn't change since there was no mapper in the chain"
        chainingResultSetMapper.map(resultSet) == resultSet

    }

    @Unroll
    def "ChainingResultSetMapper overridden map methods were never called as they are NoOP"() {
        ResultSetSchema schema = new ResultSetSchema(DefaultTimeGrain.DAY, [column].toSet())
        Result result = new Result(
                [:],
                [(column): 1.5 as BigDecimal] as Map<MetricColumn, Object>,
                new DateTime()
        )
        ResultSet resultSet = new ResultSet(schema,[result] as List)

        when: " map method of chainingResultSetMapper is invoked"
        chainingResultSetMapper.map(resultSet)

        then: "NoOP methods were never invoked"
        0 * chainingResultSetMapper.map(schema)
        0 * chainingResultSetMapper.map(result, schema)
    }

    @Unroll
    def "NoOp mappers are filtered on construction time"() {
        given: "A list of mappers with few NoOp Mappers"
        NoOpResultSetMapper noOpResultSetMapper1 = new NoOpResultSetMapper();
        NoOpResultSetMapper noOpResultSetMapper2 = new NoOpResultSetMapper();
        List<ResultSetMapper> chainedMappers = Arrays.asList(noOpResultSetMapper1,noOpResultSetMapper2)

        when:
        ChainingResultSetMapper chainingResultSetMapper = new ChainingResultSetMapper(chainedMappers)

        then: "all NoOP ResultSetMappers are filterd out"
        chainingResultSetMapper.getMappersList().size() == 0
    }

    @Unroll
    def "when map method of ChainingResultSetMapper is called, corresponding map methods are invoked in correct order"() {
        given: "A list of mappers to be chained"
        SketchRoundUpMapper sketchRoundUpMapper1 = Mock()
        SketchRoundUpMapper sketchRoundUpMapper2 = Mock()
        SketchRoundUpMapper sketchRoundUpMapper3 = Mock()
        NoOpResultSetMapper noOpResultSetMapper1 = Mock()

        List<ResultSetMapper> chainedMappers = Arrays.asList(sketchRoundUpMapper1, sketchRoundUpMapper2, sketchRoundUpMapper3)
        ChainingResultSetMapper chainingResultSetMapper = new ChainingResultSetMapper(chainedMappers)

       ResultSet mockResultSet = Mock()

        when: "map method is called on chaining instance"
        chainingResultSetMapper.map(mockResultSet)

        then: "Noop was filtered from the list"
        0 * noOpResultSetMapper1.map(mockResultSet) >> mockResultSet

        then: "sketchRoundUpMapper1 is called first"
        1 * sketchRoundUpMapper1.map(mockResultSet) >> mockResultSet

        then: "sketchRoundUpMapper2 is called second"
        1 * sketchRoundUpMapper2.map(mockResultSet) >> mockResultSet

        then: "sketchRoundUpMapper3 is called last"
        1 * sketchRoundUpMapper3.map(mockResultSet) >> mockResultSet
    }

    @Unroll
    def "when Chained Mapper instance is passed into input list, it correctly flatten the list"() {
        given: "A list of mappers with few NoOp Mappers"
        SketchRoundUpMapper sketchRoundUpMapper1 = Mock()
        SketchRoundUpMapper sketchRoundUpMapper2 = Mock()
        SketchRoundUpMapper sketchRoundUpMapper3 = Mock()
        NoOpResultSetMapper noOpResultSetMapper1 = Mock()

        ChainingResultSetMapper chainedMapper = new ChainingResultSetMapper(
                [sketchRoundUpMapper1, sketchRoundUpMapper2, noOpResultSetMapper1])

        when: "When one of the mapper in list is already instance of ChainingResultSetMapper "
        ChainingResultSetMapper mapper = ChainingResultSetMapper.createAndRenameResultSetMapperChain(
                new LogicalMetricInfo("metric column"),
                chainedMapper,
                sketchRoundUpMapper3
        )

        then: "Input dependent ChainingResultSetMapper is flatten properly and NoOp is filtered out"
        mapper.getMappersList().size() == 3

    }
}
