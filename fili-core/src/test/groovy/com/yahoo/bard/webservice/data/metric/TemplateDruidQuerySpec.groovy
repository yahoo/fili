// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric

import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.CountAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.MaxAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation

import spock.lang.Specification

class TemplateDruidQuerySpec extends Specification {
    def "verify query.depth()"() {
        setup:
            Set<Aggregation> aggs = []
            Set<PostAggregation> postAggs = []

            TemplateDruidQuery q1 = new TemplateDruidQuery(aggs, postAggs, (ZonelessTimeGrain) null)
            TemplateDruidQuery q2 = new TemplateDruidQuery(aggs, postAggs, q1, (ZonelessTimeGrain) null)
            TemplateDruidQuery q3 = new TemplateDruidQuery(aggs, postAggs, q2, (ZonelessTimeGrain) null)

        expect:
            q1.depth() == 1
            q2.depth() == 2
            q3.depth() == 3
    }

    def "verify query.isNested()"() {
        setup:
            Set<Aggregation> aggs = []
            Set<PostAggregation> postAggs = []

            TemplateDruidQuery q1 = new TemplateDruidQuery(aggs, postAggs, (ZonelessTimeGrain) null)
            TemplateDruidQuery q2 = new TemplateDruidQuery(aggs, postAggs, q1, (ZonelessTimeGrain) null)
            TemplateDruidQuery q3 = new TemplateDruidQuery(aggs, postAggs, q2, (ZonelessTimeGrain) null)

        expect:
            q1.isNested() == false
            q2.isNested() == true
            q3.isNested() == true
            q2.getInnerQuery().isNested() == false
            q3.getInnerQuery().isNested() == true
            q3.getInnerQuery().getInnerQuery().isNested() == false
            q1.isTimeGrainValid() == true
    }

    def "verify query.nest()"() {
        setup:
            Aggregation agg1 = new LongSumAggregation("field1", "field1")
            Aggregation agg2 = new MaxAggregation("field2", "field2")
            PostAggregation postagg1 = new FieldAccessorPostAggregation(agg1)
            PostAggregation postagg2 = new FieldAccessorPostAggregation(agg2)
            PostAggregation postagg3 = new ArithmeticPostAggregation("field3", ArithmeticPostAggregationFunction.PLUS,
                    [postagg1, postagg2]
            )
            TemplateDruidQuery q1 = new TemplateDruidQuery([agg1,agg2] as Set, [postagg3] as Set, (ZonelessTimeGrain) null)
            TemplateDruidQuery q2 = q1.nest()

        expect:
            q2.is(q1) == false //q2 should be a new object
            q2.getAggregations().sort() == [agg1, agg2].sort()
            q2.getPostAggregations().sort() == [postagg3].sort()
            q2.getInnerQuery() != null
            q2.getInnerQuery().getAggregations().sort() == [agg1, agg2].sort()
            q2.getInnerQuery().getPostAggregations().isEmpty()
    }

    def "verify q1.merge(q2) equals merged"() {
         setup:
            Aggregation q1_agg1 = new LongSumAggregation("field1", "field1")
            Aggregation q1_agg2 = new MaxAggregation("field2", "field2")
            Aggregation nested_agg1 = new DoubleSumAggregation("field3", "field3")
            Aggregation q2_agg1 = new LongSumAggregation("foo", "bar")

            PostAggregation q1_postagg1 = new ArithmeticPostAggregation("field5", ArithmeticPostAggregationFunction.PLUS,
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

            TemplateDruidQuery merged = q1.merge(q2)

         expect:
            //q2_agg1 is nested where the outer query is now (name="foo", fieldName="foo)
            merged.getAggregations().sort() == [q1_agg1, q1_agg2, new LongSumAggregation("foo", "foo")].sort()
            merged.getPostAggregations().sort() == [q1_postagg1, q2_postagg1].sort()
            merged.depth() == 2
            merged.getInnerQuery().getAggregations().sort() == [q2_agg1, nested_agg1].sort()
            merged.getInnerQuery().getPostAggregations().sort() == [nested_postagg1]

            q1.merge(q2) == q2.merge(q1)
    }

    def "verify q1.merge(q2) fails for duplicate aggregation names"() {
        setup:
            Aggregation q1_agg1 = new LongSumAggregation("field1", "field1")
            Aggregation q1_agg2 = new MaxAggregation("field2", "field2")
            Aggregation nested_agg1 = new DoubleSumAggregation("duplicate", "duplicate")

            Aggregation q2_agg1 = new LongSumAggregation("duplicate", "duplicate")

            TemplateDruidQuery nested = new TemplateDruidQuery([nested_agg1] as Set, [] as Set, (ZonelessTimeGrain) null)
            TemplateDruidQuery q1 = new TemplateDruidQuery([q1_agg1, q1_agg2] as Set, [] as Set, nested, (ZonelessTimeGrain) null)
            TemplateDruidQuery q2 = new TemplateDruidQuery([q2_agg1] as Set, [] as Set, (ZonelessTimeGrain) null)

        when:
            q1.merge(q2)

        then:
            thrown(IllegalArgumentException)
    }

    def "verify q1.merge(q2) fails for duplicate post aggregation names"() {
        setup:
            Aggregation q1_agg1 = new LongSumAggregation("field1", "field1")
            Aggregation q1_agg2 = new MaxAggregation("field2", "field2")
            Aggregation nested_agg1 = new DoubleSumAggregation("field3", "field3")

            Aggregation q2_agg1 = new LongSumAggregation("field4", "field4")

            PostAggregation q1_postagg1 = new ArithmeticPostAggregation("duplicate", ArithmeticPostAggregationFunction.PLUS,
                    [
                           new FieldAccessorPostAggregation(q1_agg1),
                           new FieldAccessorPostAggregation(q1_agg2)
                    ]
            )

            PostAggregation nested_postagg1 = new ConstantPostAggregation("field5", 100)
            PostAggregation q2_postagg1 = new ConstantPostAggregation("duplicate", 100)

            TemplateDruidQuery nested = new TemplateDruidQuery([nested_agg1] as Set, [nested_postagg1] as Set, (ZonelessTimeGrain) null)
            TemplateDruidQuery q1 = new TemplateDruidQuery([q1_agg1, q1_agg2] as Set, [q1_postagg1] as Set, nested, (ZonelessTimeGrain) null)
            TemplateDruidQuery q2 = new TemplateDruidQuery([q2_agg1] as Set, [q2_postagg1] as Set, (ZonelessTimeGrain) null)

        when:
            q1.merge(q2)

        then:
            thrown(IllegalArgumentException)
    }

    def "verify TemplateDruidQuery throws IllegalArgumentException for a duplicate agg & postagg name"() {
        setup:
            Aggregation a1 = new CountAggregation("a1")
            Aggregation a2 = new CountAggregation("dup")
            Aggregation a3 = new CountAggregation("a3")

            PostAggregation p1 = new ConstantPostAggregation("p1", 100)
            PostAggregation p2 = new ConstantPostAggregation("p2", 100)
            PostAggregation p3 = new ConstantPostAggregation("dup", 100)

        when:
            new TemplateDruidQuery([a1,a2,a3] as Set, [p1,p2,p3] as Set, (ZonelessTimeGrain) null)

        then:
            thrown(IllegalArgumentException)
    }
}
