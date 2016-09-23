// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation

import spock.lang.Specification

/**
 *  Test default methods on WithFields interface
 */
class WithFieldsSpec extends Specification {

    WithFields<ArithmeticPostAggregation> postAggregation;
    FieldAccessorPostAggregation field1
    FieldAccessorPostAggregation field2
    Aggregation aggregation1 = Mock(Aggregation)
    Aggregation aggregation2 = Mock(Aggregation)
    Dimension dimension1 = Mock(Dimension)
    Dimension dimension2 = Mock(Dimension)


    def setup() {
        field1 = new FieldAccessorPostAggregation(aggregation1)
        field2 = new FieldAccessorPostAggregation(aggregation2)

        aggregation1.dependentDimensions >> ([dimension1] as Set)
        aggregation2.dependentDimensions >> ([dimension2] as Set)
        postAggregation = new ArithmeticPostAggregation(
                "test", ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MINUS,
                [field1, field2]
        );
    }

    def "Test dependant dimensions resolve from fields"() {
        expect:
        postAggregation.dependentDimensions == ([dimension1, dimension2] as Set)
    }
}
