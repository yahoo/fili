// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation

import spock.lang.Specification

/**
 *  Tests for field access post aggregation spec
 */
class FieldAccessorPostAggregationSpec extends Specification {

    Aggregation aggregation = Mock(Aggregation)
    PostAggregation fieldPostAggregation
    Dimension dimension = Mock(Dimension)

    def setup() {
        fieldPostAggregation = new FieldAccessorPostAggregation(aggregation)
    }

   def  "Test get dependant dimensions pulls dimensions from aggregation"() {
        setup:
        aggregation.dependentDimensions >> [dimension]

        expect:
        fieldPostAggregation.dependentDimensions == ([dimension] as Set)
    }
}
