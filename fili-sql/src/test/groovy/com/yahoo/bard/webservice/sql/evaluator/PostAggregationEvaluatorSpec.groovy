// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator

import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE
import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MINUS
import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MULTIPLY
import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS
import static com.yahoo.bard.webservice.sql.builders.Aggregator.sum
import static com.yahoo.bard.webservice.sql.builders.PostAggregator.arithmetic
import static com.yahoo.bard.webservice.sql.builders.PostAggregator.constant
import static com.yahoo.bard.webservice.sql.builders.PostAggregator.field

import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchEstimatePostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation

import spock.lang.Specification
import spock.lang.Unroll

class PostAggregationEvaluatorSpec extends Specification {
    static Map<String, String> fieldToValue = [:]
    static String ONE = "one"
    static String FIVE = "five"
    static PostAggregationEvaluator postAggregationEvaluator = new PostAggregationEvaluator()

    void setup() {
        fieldToValue.put(ONE, "1")
        fieldToValue.put(FIVE, "5")
    }

    @Unroll
    def "Evaluate Post Aggregations expecting #value"() {
        expect:
        Number result = postAggregationEvaluator.calculate(postAgg, { it -> fieldToValue.get(it) })
        result == value

        where: "given"
        postAgg                                                       | value
        arithmetic(PLUS, field(sum(ONE)))                             | 1
        arithmetic(PLUS, constant(1))                                 | 1
        arithmetic(PLUS, arithmetic(PLUS, constant(1)), constant(1))  | 2
        arithmetic(PLUS, field(sum(ONE)), field(sum(FIVE)))           | 6
        arithmetic(MINUS, field(sum(ONE)), field(sum(FIVE)))          | -4
        arithmetic(MINUS, constant(1), constant(1), constant(1))      | -1
        arithmetic(MULTIPLY, field(sum(ONE)), field(sum(FIVE)))       | 5
        arithmetic(DIVIDE, field(sum(ONE)), field(sum(FIVE)))         | 1 / 5
        arithmetic(DIVIDE, field(sum(ONE)), constant(0))              | 0
        arithmetic(DIVIDE, constant(1), constant(1), constant(2))     | 1 / 2 // = ((1/1)/2)
        arithmetic(DIVIDE, field(sum(ONE)), constant(1), constant(0)) | 0 // = ((1/1)/0)
    }

    @Unroll
    def "Test #thrownException post aggregations and bad inputs"() {
        when:
        Number result = postAggregationEvaluator.calculate(postAgg, { it -> fieldToValue.get(it) })

        then:
        thrown thrownException

        where:
        postAgg                                                    | thrownException
        new ThetaSketchEstimatePostAggregation("", null)           | RuntimeException
        new ThetaSketchSetOperationPostAggregation("", null, null) | RuntimeException
    }
}
