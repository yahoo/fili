// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers

import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow

import spock.lang.Specification

import java.util.function.BiFunction
import java.util.function.BiPredicate

class MemoizingDimensionMappingResultSetMapperTest extends Specification {

    String keyValue = "key"
    String newKeyValue = "newKey"
    String nonKeyValue = "nonKey"
    String keyFieldName = "keyFieldName"
    String nonKeyFieldName = "nonKeyFieldName"
    DimensionField keyField = Mock(DimensionField)
    DimensionField nonKeyField = Mock(DimensionField)


    DimensionRow unmodifiedTestRow = new DimensionRow(keyField, [(keyField): keyValue, (nonKeyField): nonKeyValue])
    DimensionRow modifiedTestRow = new DimensionRow(keyField, [(keyField): newKeyValue, (nonKeyField): nonKeyValue])

    DimensionColumn dimensionColumnMatch
    DimensionColumn dimensionColumnNonMatch
    DimensionColumn dimensionColumnNonMatch2


    Result resultMatch = Mock(Result)
    Result resultNonMatch = Mock(Result)
    ResultSetSchema schema = Mock(ResultSetSchema)
    Dimension matchingDimension = Mock(Dimension)

    BiPredicate<DimensionColumn, DimensionRow> matcher
    BiFunction<DimensionColumn, DimensionRow, AbstractMap.SimpleEntry<DimensionColumn, DimensionRow>> mapper

    def setup() {
        keyField.name >> keyFieldName
        nonKeyField.name >> nonKeyField

        matchingDimension.getApiName() >> "match"
        Dimension nonMatchingDimension = Mock(Dimension)
        Dimension nonMatchingDimension2 = Mock(Dimension)

        nonMatchingDimension.getApiName() >> "nonMatch1"
        nonMatchingDimension2.getApiName() >> "nonMatch2"

        nonMatchingDimension.equals(_) >> { Dimension dim -> !"match".equals(dim.getApiName()) }
        matchingDimension.equals(_) >> { Dimension dimension -> "match".equals(dimension.getApiName()) }

        dimensionColumnMatch = new DimensionColumn(matchingDimension)
        dimensionColumnNonMatch = new DimensionColumn(nonMatchingDimension)
        dimensionColumnNonMatch2 = new DimensionColumn(nonMatchingDimension2)

        resultMatch.getMetricValues() >> [:]
        resultNonMatch.getMetricValues() >> [:]

        resultMatch.getDimensionRows() >> [(dimensionColumnMatch): unmodifiedTestRow, (dimensionColumnNonMatch):
                unmodifiedTestRow]
        resultNonMatch.getDimensionRows() >> [(dimensionColumnNonMatch): unmodifiedTestRow, (dimensionColumnNonMatch2):
                unmodifiedTestRow]
    }

    def "Matching columns transform, non matching do not"() {
        matcher = Mock(BiPredicate)
        mapper = Mock(BiFunction)

        MemoizingDimensionMappingResultSetMapper memoMapper = new MemoizingDimensionMappingResultSetMapper(
                matcher,
                mapper,
                false
        )

        when:
        Result actual = memoMapper.map(resultMatch, schema)
        Result actualNonMatch = memoMapper.map(resultNonMatch, schema)

        then:
        4 * matcher.test(_, _) >> { DimensionColumn dimensionColumn, DimensionRow dimRow ->
            matchingDimension.equals(dimensionColumn.getDimension())
        }
        1 * mapper.apply(_, _) >> { DimensionColumn dimensionColumn, DimensionRow dimRow ->
            if (dimensionColumn.getDimension() == matchingDimension) {
                return new AbstractMap.SimpleEntry<DimensionColumn, DimensionRow>(dimensionColumn, modifiedTestRow)
            }
            return new AbstractMap.SimpleEntry<DimensionColumn, DimensionRow>(dimensionColumn, dimensionRow)
        }

        and:
        actual.getDimensionRow(dimensionColumnMatch) == modifiedTestRow
        actual.getDimensionRow(dimensionColumnNonMatch) == unmodifiedTestRow
        actualNonMatch.getDimensionRow(dimensionColumnNonMatch) == unmodifiedTestRow
        actualNonMatch.getDimensionRow(dimensionColumnNonMatch2) == unmodifiedTestRow
    }

    def "Test memoization skips previously discovered rows"() {
        matcher = Mock(BiPredicate)
        mapper = Mock(BiFunction)

        MemoizingDimensionMappingResultSetMapper memoMapper = new MemoizingDimensionMappingResultSetMapper(
                matcher,
                mapper
        )

        when:
        Result actual = memoMapper.map(resultMatch, schema)

        then:
        2 * matcher.test(_, _) >> { DimensionColumn dimensionColumn, DimensionRow dimRow ->
            matchingDimension.equals(dimensionColumn.getDimension())
        }
        1 * mapper.apply(_, _) >> { DimensionColumn dimensionColumn, DimensionRow dimRow ->
            if (dimensionColumn.getDimension() == matchingDimension) {
                return new AbstractMap.SimpleEntry<DimensionColumn, DimensionRow>(dimensionColumn, modifiedTestRow)
            }
            return new AbstractMap.SimpleEntry<DimensionColumn, DimensionRow>(dimensionColumn, dimensionRow)
        }
        actual.getDimensionRow(dimensionColumnMatch) == modifiedTestRow
        actual.getDimensionRow(dimensionColumnNonMatch) == unmodifiedTestRow


        when:
        actual = memoMapper.map(resultMatch, schema)

        then:
        0 * matcher.test(_, _)
        0 * mapper.apply(_, _)
        actual.getDimensionRow(dimensionColumnMatch) == modifiedTestRow
        actual.getDimensionRow(dimensionColumnNonMatch) == unmodifiedTestRow

        when:
        Result actualNonMatch = memoMapper.map(resultNonMatch, schema)

        then:
        1 * matcher.test(_, _) >> { DimensionColumn dimensionColumn, DimensionRow dimRow ->
            matchingDimension.equals(dimensionColumn.getDimension())
        }
        actualNonMatch.getDimensionRow(dimensionColumnNonMatch) == unmodifiedTestRow
        actualNonMatch.getDimensionRow(dimensionColumnNonMatch2) == unmodifiedTestRow
    }

    def "Test dimension key transformer case"() {
        BiFunction fieldMapper = Mock(BiFunction)
        fieldMapper.apply(_, _) >> { DimensionField dimensionField, String oldValue ->
            (dimensionField.name == keyFieldName) ? (oldValue + oldValue) : oldValue
        }

        MemoizingDimensionMappingResultSetMapper mapper = MemoizingDimensionMappingResultSetMapper.buildFromFieldMapper(
                matchingDimension,
                fieldMapper
        )

        when:
        Result actual = mapper.map(resultMatch, schema)

        then:
        actual.getDimensionRow(dimensionColumnMatch).get(keyField) == "keykey"
        actual.getDimensionRow(dimensionColumnMatch).get(nonKeyField) == nonKeyValue
        actual.getDimensionRow(dimensionColumnNonMatch) == unmodifiedTestRow
    }
}
