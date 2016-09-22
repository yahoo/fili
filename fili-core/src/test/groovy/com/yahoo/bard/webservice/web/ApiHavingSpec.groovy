// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary

import spock.lang.Specification
import spock.lang.Unroll

class ApiHavingSpec extends Specification {

    MetricDictionary metricStore
    LogicalMetric metric1
    LogicalMetric metric2
    LogicalMetric metric3

    def setup() {
        metric1 = new LogicalMetric(null, null, "metric1")
        metric2 = new LogicalMetric(null, null, "metric2")
        metric3 = new LogicalMetric(null, null, "metric3")

        metricStore = new MetricDictionary()
        metricStore.add(metric1)
        metricStore.add(metric2)
        metricStore.add(metric3)
    }

    @Unroll
    def "Good having query #metric-#op#values parses correctly"() {

        given:
        String query = "$metric-$op$values"

        when:
        ApiHaving having = new ApiHaving(query, metricStore)

        then:
        having.metric?.name == metric
        having.operation == HavingOperation.fromString(op)
        having.values == expected as Set

        where:
        metric    | op               | values                | expected
        'metric1' | 'eq'             | '[1,2,3]'             | [1, 2, 3]
        'metric1' | 'gt'             | '[4,5,6]'             | [4, 5, 6]
        'metric1' | 'lt'             | '[7,8,9]'             | [7, 8, 9]

        // all operations/aliases; some of these are duplicated from above
        'metric1' | 'equalTo'        | '[1,2,3]'             | [1, 2, 3]
        'metric1' | 'equals'         | '[1,2,3]'             | [1, 2, 3]
        'metric1' | 'eq'             | '[1,2,3]'             | [1, 2, 3]
        'metric1' | 'greaterThan'    | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'greater'        | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'gt'             | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'lessThan'       | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'less'           | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'lt'             | '[7,8,9]'             | [7, 8, 9]

        'metric1' | 'notEqualTo'     | '[1,2,3]'             | [1, 2, 3]
        'metric1' | 'notEquals'      | '[1,2,3]'             | [1, 2, 3]
        'metric1' | 'noteq'          | '[1,2,3]'             | [1, 2, 3]
        'metric1' | 'notGreaterThan' | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'notGreater'     | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'notgt'          | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'lte'            | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'notLessThan'    | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'notLess'        | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'notlt'          | '[7,8,9]'             | [7, 8, 9]
        'metric1' | 'gte'            | '[7,8,9]'             | [7, 8, 9]

        // Floats, different metric names
        'metric1' | 'lt'             | '[78.8]'              | [78.8]
        'metric2' | 'eq'             | '[37.9,40.2]'         | [37.9, 40.2]
        'metric3' | 'eq'             | '[4.56,7.89,9.10]'    | [4.56, 7.89, 9.10]
        'metric1' | 'gt'             | '[4.56,7.89,9.10,47]' | [4.56, 7.89, 9.10, 47] // mixed float/integer

        // scientific notation
        'metric2' | 'eq'             | '[1e8]'               | [1e8]
        'metric2' | 'eq'             | '[1.5e8]'             | [1.5e8]

        //0 and negative numbers
        'metric2' | 'eq'             | '[0]'                 | [0]
        'metric2' | 'eq'             | '[-1]'                | [-1]
        'metric2' | 'eq'             | '[0, -1]'             | [0, -1]
        'metric2' | 'eq'             | '[-10, -20, -38]'     | [-10, -20, -38]
        'metric2' | 'eq'             | '[-10.8]'             | [-10.8]
        'metric2' | 'eq'             | '[-10.8, -20.3]'      | [-10.8, -20.3]
    }

    @Unroll
    def "Bad having query #having throws #exception.simpleName because #reason"() {

        when:
        new ApiHaving(having, metricStore)

        then:
        thrown exception

        where:
        having                        | exception          | reason
        'unknown-eq[123]'             | BadHavingException | 'Unknown Metric'
        'metric1-unknown[123]'        | BadHavingException | 'Unknown Operation'
        'metric1eq[123]'              | BadHavingException | 'Missing Dash'
        'metric1-eq123]'              | BadHavingException | 'Missing Opening Bracket'
        'metric1-eq[]'                | BadHavingException | 'Missing value list elements'
        'metric1-[123]'               | BadHavingException | 'Missing Operation'
        '-eq[123]'                    | BadHavingException | 'Missing Metric'
        'metric1-eq'                  | BadHavingException | 'Missing value list'

        'unknown-eq[123,456]'         | BadHavingException | 'Unknown Metric (multi-value)'
        'metric1-unknown[123,456]'    | BadHavingException | 'Unknown Operation (multi-value)'
        'metric1eq[123,456]'          | BadHavingException | 'Missing Dash (multi-value)'
        'metric1-eq123,456]'          | BadHavingException | 'Missing Opening Bracket (multi-value)'
        'metric1-[123,456]'           | BadHavingException | 'Missing Operation (multi-value)'
        '-eq[123,456]'                | BadHavingException | 'Missing Metric (multi-value)'

        'metric1-eq[,123]'            | BadHavingException | 'Having requests empty string'
        'metric1-eq[123,]'            | BadHavingException | 'Having requests empty string'
        'metric1-eq[1,,2]'            | BadHavingException | 'Having requests empty string'
        'metric1-eq[2, ,2]'           | BadHavingException | 'Having requests empty string'
        'metric1-eq[,]'               | BadHavingException | 'Having requests empty string'
        'metric1-eq[,,]'              | BadHavingException | 'Having requests empty string'
        'metric1-eq[ ]'               | BadHavingException | 'Having requests empty string'
        'metric1-eq[  ]'              | BadHavingException | 'Having requests empty string'
        'metric1-eq[]'                | BadHavingException | 'Having requests empty string'

        'metric1-eq[foo]'             | BadHavingException | 'Non-numeric value'
        'metric1-eq[bed]'             | BadHavingException | 'Non-numeric (hex) value'
        'metric1-eq[0xbed]'           | BadHavingException | 'Non-numeric (hex) value'
        'metric1-eq[foo.bar]'         | BadHavingException | 'Non-numeric value'
        'metric1-eq[123,foo.bar]'     | BadHavingException | 'Non-numeric value (multi-value)'
        'metric1-eq[foo.bar,123]'     | BadHavingException | 'Non-numeric value (multi-value)'
        'metric1-eq[foo.bar,bar.foo]' | BadHavingException | 'Non-numeric value (multi-value)'
        'metric1-eq[1foo]'            | BadHavingException | 'Non-numeric value (multi-value)'
    }
}
