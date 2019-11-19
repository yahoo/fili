// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import org.apache.commons.lang3.tuple.Pair

import spock.lang.Specification

class CountAggregationSpec extends Specification {

    def "verify count aggregation nests correctly"() {
        setup:
            Aggregation a1 = new CountAggregation("name")

            Aggregation outer = new LongSumAggregation("name", "name")
            Aggregation inner = new CountAggregation("name")

        when:
        Pair<Optional<Aggregation>, Optional<Aggregation>> nested = a1.nest()

        then:
        nested.getLeft().get() == outer
        nested.getRight().get() == inner
    }
}
