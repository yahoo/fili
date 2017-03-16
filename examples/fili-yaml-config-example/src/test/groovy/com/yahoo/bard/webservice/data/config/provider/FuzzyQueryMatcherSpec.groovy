// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation

import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS
import spock.lang.Specification

public class FuzzyQueryMatcherSpec extends Specification {
    def "identical empty TemplateDruidQueries should match"() {
        def actual = new TemplateDruidQuery(
                [],
                []
        )
        expect:
        FuzzyQueryMatcher.matches(actual, actual)
    }

    def "identical non-empty TemplateDruidQueries should match"() {
        def agg1 = new LongSumAggregation("longsum1", "field1")
        def actual = new TemplateDruidQuery(
                [agg1, new LongSumAggregation("longsum2", "field2")],
                [new ConstantPostAggregation("constant1", 3.5), new FieldAccessorPostAggregation(agg1)]
        )
        expect:
        FuzzyQueryMatcher.matches(actual, actual)
    }

    def "not-quite identical TemplateDruidQueries should match"() {
        def agg1 = new LongSumAggregation("longsum1", "field1")
        def actual = new TemplateDruidQuery(
                [agg1],
                [new ConstantPostAggregation("constant1", 3.5),
                 new ArithmeticPostAggregation("some-arithmetic", PLUS, [new ConstantPostAggregation("35", 3.5), new ConstantPostAggregation("45", 4.5)])
                ]
        )
        def expected = new TemplateDruidQuery(
                [agg1],
                [new ConstantPostAggregation("constant1", 3.5),
                 new ArithmeticPostAggregation("some-arithmetic", PLUS, [new ConstantPostAggregation("35_2", 3.5), new ConstantPostAggregation("45_2", 4.5)])
                ]
        )
        expect:
        FuzzyQueryMatcher.matches(actual, expected)
    }

    def "no match if values differ"() {
        def agg1 = new LongSumAggregation("longsum1", "field1")
        def actual = new TemplateDruidQuery(
                [agg1],
                [new ConstantPostAggregation("constant1", 3.5),
                 new ArithmeticPostAggregation("some-arithmetic", PLUS, [new ConstantPostAggregation("35", 3.5), new ConstantPostAggregation("45", 4.5)])
                ]
        )

        def expected = new TemplateDruidQuery(
                [agg1],
                [new ConstantPostAggregation("constant1", 3.5),
                 new ArithmeticPostAggregation("some-arithmetic", PLUS, [new ConstantPostAggregation("35_2", 3.3), new ConstantPostAggregation("45_2", 4.5)])
                ]
        )


        when:
        FuzzyQueryMatcher.matches(actual, expected)

        then:
        RuntimeException ex = thrown()
        ex.message =~ /Expected.*3\.3.*actual.*3\.5/
    }
}
