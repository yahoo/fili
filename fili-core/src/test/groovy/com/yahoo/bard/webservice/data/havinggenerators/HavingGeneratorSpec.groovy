// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.havinggenerators


import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.web.ApiHaving
import com.yahoo.bard.webservice.web.HavingOperation
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.generator.having.antlr.AntlrHavingGenerator

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class HavingGeneratorSpec extends Specification {
    @Shared MetricDictionary metricDictionary
    @Shared LogicalMetric metric1
    @Shared LogicalMetric metric2
    @Shared LogicalMetric metric3
    @Shared Set<LogicalMetric> logicalMetrics

    def setup() {
        metric1 = new LogicalMetricImpl(null, null, "metric1")
        metric2 = new LogicalMetricImpl(null, null, "metric2")
        metric3 = new LogicalMetricImpl(null, null, "metric3")

        LogicalMetric metric4Base = new LogicalMetricImpl(null, null, "metric4")
        LogicalMetric metric4 = new LogicalMetricImpl(null, null, "metric4(foo=bar)")
        LogicalMetric metric5Base = new LogicalMetricImpl(null, null, "metric5")
        LogicalMetric metric5 = new LogicalMetricImpl(null, null, "metric5(foo=bar,foo2=bar2)")

        List<LogicalMetric> metrics = [metric1, metric2, metric3]

        metricDictionary = new MetricDictionary()
        metrics.every() { metricDictionary.add(it) }
        metricDictionary.add(metric4Base)
        metricDictionary.add(metric5Base)

        logicalMetrics = new HashSet<>()
        logicalMetrics.addAll(metrics)
        logicalMetrics.add(metric4)
        logicalMetrics.add(metric5)
    }

    @Unroll
    def "check parsing generateHavings for one metric"() {

        given:
        String query = "$metric-$op$values"

        when:
        Map<LogicalMetric, Set<ApiHaving>> generateHaving =  new AntlrHavingGenerator(metricDictionary).apply(query, logicalMetrics);

        then:
        Set<ApiHaving> havings = generateHaving.get(metric1)
        havings[0].metric?.name == metric
        havings[0].operation == HavingOperation.fromString(op)
        havings[0].values == expected as List

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
        'metric1' | 'eq'             | '[37.9,40.2]'         | [37.9, 40.2]
        'metric1' | 'eq'             | '[4.56,7.89,9.10]'    | [4.56, 7.89, 9.10]
        'metric1' | 'gt'             | '[4.56,7.89,9.10,47]' | [4.56, 7.89, 9.10, 47] // mixed float/integer

        // scientific notation
        'metric1' | 'eq'             | '[1e8]'               | [1e8]
        'metric1' | 'eq'             | '[1.5e8]'             | [1.5e8]

        //0 and negative numbers
        'metric1' | 'eq'             | '[0]'                 | [0]
        'metric1' | 'eq'             | '[-1]'                | [-1]
        'metric1' | 'eq'             | '[0, -1]'             | [0, -1]
        'metric1' | 'eq'             | '[-10, -20, -38]'     | [-10, -20, -38]
        'metric1' | 'eq'             | '[-10.8]'             | [-10.8]
        'metric1' | 'eq'             | '[-10.8, -20.3]'      | [-10.8, -20.3]

        //whitespace between values are allowed
        'metric1' | 'eq'             | '[  0,1.8  ]'         | [0,1.8]
        'metric1' | 'bet'            | '[  2.0,  -.8  ]'     | [2.0,-0.8]

        //more valid doubles
        'metric1' | 'eq'            | '[.1]'                 | [0.1]
        'metric1' | 'eq'            | '[1.]'                 | [1.0]
        'metric1' | 'eq'            | '[1.4]'                | [1.4]
        'metric1' | 'eq'            | '[-1.]'                | [-1.0]
        'metric1' | 'eq'            | '[-.1]'                | [-0.1]
        'metric1' | 'eq'            | '[1.e1]'               |  [10]
        'metric1' | 'eq'            | '[0.e1]'               |  [0]
        'metric1' | 'eq'            | '[.0e1]'               |  [0]

    }
    @Unroll
    def "check parsing generateHavings for one protocol metrics"() {

        given:
        String query = "$metric-$op$values"

        when:
        Map<LogicalMetric, Set<ApiHaving>> generateHaving =  new AntlrHavingGenerator(metricDictionary).apply(query, logicalMetrics);

        then:
        Set<ApiHaving> havings = generateHaving.values().stream().findFirst().get()
        havings[0].metric?.name == metric
        havings[0].operation == HavingOperation.fromString(op)
        havings[0].values == expected as List

        where:
        metric                       | op   | values    | expected
        'metric4(foo=bar)'           | 'eq' | '[1,2,3]' | [1, 2, 3]
        'metric5(foo=bar,foo2=bar2)' | 'eq' | '[1,2,3]' | [1, 2, 3]
    }

    @Unroll
    def "check parsing generateHaving for multiple metric"() {
        given:
        String query = "metric1-gt[25],metric3-notLessThan[1,2,3]"

        when:
        Map<LogicalMetric, Set<ApiHaving>> generateHaving =  new AntlrHavingGenerator(metricDictionary).apply(query, logicalMetrics);

        then:
        generateHaving.containsKey(metric1)
        generateHaving.containsKey(metric3)

        Set<ApiHaving> havingsMetric1 = generateHaving.get(metric1)
        havingsMetric1[0].metric?.name == 'metric1'
        havingsMetric1[0].operation == HavingOperation.fromString('gt')
        havingsMetric1[0].values == [25] as List

        Set<ApiHaving> havingsMetric3 = generateHaving.get(metric3)
        havingsMetric3[0].metric?.name == 'metric3'
        havingsMetric3[0].operation == HavingOperation.fromString('notLessThan')
        havingsMetric3[0].values == [1,2,3] as List
    }

    @Unroll
    def "check parsing generateHaving for one metric with multiple operations"() {
        given:
        String query = "metric1-notEqualTo[-2.4],metric1-gte[8.0,23]"

        when:
        Map<LogicalMetric, Set<ApiHaving>> generateHaving =  new AntlrHavingGenerator(metricDictionary).apply(query, logicalMetrics);

        then:
        generateHaving.containsKey(metric1)

        Set<ApiHaving> havingsMetric1 = generateHaving.get(metric1)
        havingsMetric1[0].metric?.name == 'metric1'
        havingsMetric1[0].operation == HavingOperation.fromString('notEqualTo')
        havingsMetric1[0].values == [-2.4] as List

        havingsMetric1[1].metric?.name == 'metric1'
        havingsMetric1[1].operation == HavingOperation.fromString('gte')
        havingsMetric1[1].values == [8.0,23] as List
    }

    @Unroll
    def "Bad having query #having throws #exception.simpleName because #reason"() {

        when:
        AntlrHavingGenerator digitsHavingGenerator = new AntlrHavingGenerator(metricDictionary);
        digitsHavingGenerator.apply(having, logicalMetrics);

        then:
        thrown exception

        where:
        having                        | exception              | reason

        'unknown-noteq[123]'          | BadApiRequestException | 'Unknown Metric'
        'metric1-unknown[123]'        | BadApiRequestException | 'Unknown Operation'
        'metric1eq[123]'              | BadApiRequestException | 'Missing Dash'
        'metric1-eq123]'              | BadApiRequestException | 'Missing Opening Bracket'
        'metric1-eq[]'                | BadApiRequestException | 'Missing value list elements'
        'metric1-[123]'               | BadApiRequestException | 'Missing Operation'
        '-eq[123]'                    | BadApiRequestException | 'Missing Metric'
        'metric1-eq'                  | BadApiRequestException | 'Missing value list'

        'unknown-eq[123,456]'         | BadApiRequestException | 'Unknown Metric (multi-value)'
        'metric1-unknown[123,456]'    | BadApiRequestException | 'Unknown Operation (multi-value)'
        'metric1eq[123,456]'          | BadApiRequestException | 'Missing Dash (multi-value)'
        'metric1-eq123,456]'          | BadApiRequestException | 'Missing Opening Bracket (multi-value)'
        'metric1-[123,456]'           | BadApiRequestException | 'Missing Operation (multi-value)'
        '-eq[123,456]'                | BadApiRequestException | 'Missing Metric (multi-value)'

        'metric1-eq[,123]'            | BadApiRequestException | 'Having requests empty string'
        'metric1-eq[123,]'            | BadApiRequestException | 'Having requests empty string'
        'metric1-eq[1,,2]'            | BadApiRequestException | 'Having requests empty string'
        'metric1-eq[2, ,2]'           | BadApiRequestException | 'Having requests empty string'
        'metric1-eq[,]'               | BadApiRequestException | 'Having requests empty string'
        'metric1-eq[,,]'              | BadApiRequestException | 'Having requests empty string'
        'metric1-eq[ ]'               | BadApiRequestException | 'Having requests empty string'
        'metric1-eq[  ]'              | BadApiRequestException | 'Having requests empty string'
        'metric1-eq[]'                | BadApiRequestException | 'Having requests empty string'

        'metric1-eq[foo]'             | BadApiRequestException | 'Non-numeric value'
        'metric1-eq[bed]'             | BadApiRequestException | 'Non-numeric (hex) value'
        'metric1-eq[0xbed]'           | BadApiRequestException | 'Non-numeric (hex) value'
        'metric1-eq[foo.bar]'         | BadApiRequestException | 'Non-numeric value'
        'metric1-eq[123,foo.bar]'     | BadApiRequestException | 'Non-numeric value (multi-value)'
        'metric1-eq[foo.bar,123]'     | BadApiRequestException | 'Non-numeric value (multi-value)'
        'metric1-eq[foo.bar,bar.foo]' | BadApiRequestException | 'Non-numeric value (multi-value)'
        'metric1-eq[1foo]'            | BadApiRequestException | 'Non-numeric value (multi-value)'

        // valid but with whitespace error thrown
        'metric1-noteq[1.  0,-  2]' | BadApiRequestException   | 'no whitespace between value'

        // invalid scientifc notations
        'metric1-noteq[.e1]'        | BadApiRequestException   | 'not a valid scientific number'
        'metric1-eq[+.e1]'          | BadApiRequestException   | 'not a valid scientific number'
        'metric1-eq[e]'             | BadApiRequestException   | 'not a valid scientific number'
        'metric1-eq[e.3]'           | BadApiRequestException   | 'not a valid scientific number'
        'metric1-eq[e1]'            | BadApiRequestException   | 'not a valid scientific number'

        'metric1 -eq[.]'            | BadApiRequestException   | 'just dot is not valid'
    }
}
