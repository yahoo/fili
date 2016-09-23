// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric

import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.MaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.SketchCountAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.SketchMergeAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.web.DataApiRequest

import spock.lang.Specification

class TemplateDruidQueryMergerSpec extends Specification {
    def "Verify merger.merge"() {
        setup:
            Aggregation q1_agg1 = new LongSumAggregation("field1", "field1")
            Aggregation q1_agg2 = new MaxAggregation("field2", "field2")
            Aggregation nested_agg1 = new DoubleSumAggregation("field3", "field3")
            Aggregation q2_agg1 = new LongSumAggregation("field4", "field4")
            //TODO resolve this
            Aggregation q3_agg1 = new SketchCountAggregation("foo", "bar", 1000)

            PostAggregation q1_postagg1 = new ArithmeticPostAggregation("field3", ArithmeticPostAggregationFunction.PLUS,
                    [
                            new FieldAccessorPostAggregation(q1_agg1),
                            new FieldAccessorPostAggregation(q1_agg2)
                    ]
            )

            PostAggregation nested_postagg1 = new ConstantPostAggregation("field6", 100)
            PostAggregation q2_postagg1 = new ConstantPostAggregation("field7", 100)

            TemplateDruidQuery nested = new TemplateDruidQuery([nested_agg1] as Set, [nested_postagg1] as Set, (ZonelessTimeGrain) null)
            TemplateDruidQuery q1 = new TemplateDruidQuery([q1_agg1,q1_agg2] as Set, [q1_postagg1] as Set, nested, (ZonelessTimeGrain) null)

            TemplateDruidQuery q2 = new TemplateDruidQuery([q2_agg1] as Set, [q2_postagg1] as Set, (ZonelessTimeGrain) null)
            TemplateDruidQuery q3 = new TemplateDruidQuery([q3_agg1] as Set, [] as Set, (ZonelessTimeGrain) null)

            LogicalMetric m1 = new LogicalMetric(q1, null, "Metric1", null)
            LogicalMetric m2 = new LogicalMetric(q2, null, "Metric2", null)
            LogicalMetric m3 = new LogicalMetric(q3, null, "Metric3", null)

            DataApiRequest request = Mock(DataApiRequest)
            TemplateDruidQueryMerger merger = new TemplateDruidQueryMerger()

        when:
            TemplateDruidQuery merged = merger.merge(request)

        then:
            (1 .. _) * request.getLogicalMetrics() >> [m1,m2,m3].toSet()

            //q3_agg1 should be nested where the outer query is a sketch count (name=foo, fieldName=foo)
            merged.getAggregations().sort() == [q1_agg1,q1_agg2,q2_agg1,new SketchCountAggregation("foo", "foo", 1000)].sort()
            merged.getPostAggregations().sort() == [q1_postagg1, q2_postagg1].sort()
            //q3_agg1 should be nested where the inner query is a sketch merge (name=foo, fieldName=bar)
            merged.getInnerQuery().getAggregations().sort() == [q2_agg1,new SketchMergeAggregation("foo", "bar", 1000),nested_agg1].sort()
            merged.getInnerQuery().getPostAggregations().sort() == [nested_postagg1]
            merged.depth() == 2
    }


    def "Verify merger.merge throws exception for empty list of metric"() {
        setup:
            DataApiRequest request = Mock(DataApiRequest)
            TemplateDruidQueryMerger merger = new TemplateDruidQueryMerger()

        when:
            TemplateDruidQuery merged = merger.merge(request)

        then:
            (1 .. _) * request.getLogicalMetrics() >> [].toSet()
            thrown(IllegalStateException)
    }
}
