// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.FilterOperation

import spock.lang.Specification

import java.util.function.Function
import java.util.function.Predicate

class FilterFactorySpec extends Specification {

    FilterFactory filterFactory = new FilterFactory()

    Dimension dimension = Mock(Dimension)
    DimensionField dimensionField = Mock(DimensionField)
    FilterOperation filterOperation = Mock(FilterOperation)
    List<String> values = []

    class TestApiFilter extends ApiFilter {

        TestApiFilter(
                final Dimension dimension,
                final DimensionField dimensionField,
                final FilterOperation operation,
                final Collection<String> values
        ) {
            super(dimension, dimensionField, operation, values)
        }
    }

    def "Build through proxy"() {
        given:
        FilterFactory.FilterFactoryFunction factoryFunction = { FilterFactory.FilterComponents components ->
            new TestApiFilter(
                components.dimension,
                components.dimensionField,
                components.operation,
                components.values
        )}
        Map.Entry<Predicate<?>, Function<?,?>> foo = new AbstractMap.SimpleEntry(
                (java.util.function.Predicate) { foo
                    ->
                    true
                }, factoryFunction
        )
        filterFactory.getFilterFactoryProviders().add(foo)

        expect:
        ApiFilter result = filterFactory.buildFilter(dimension, dimensionField, filterOperation, values)
        result instanceof TestApiFilter
        result.getDimension() == dimension
        result.getDimensionField() == dimensionField
        result.getOperation() == filterOperation
        result.getValues().isEmpty()
    }

    def "Build default ApiFilter"() {
        expect:
        ApiFilter result = filterFactory.buildFilter(dimension, dimensionField, filterOperation, values)
        result.getDimension() == dimension
        result.getDimensionField() == dimensionField
        result.getOperation() == filterOperation
        result.getValues().isEmpty()
    }
}
