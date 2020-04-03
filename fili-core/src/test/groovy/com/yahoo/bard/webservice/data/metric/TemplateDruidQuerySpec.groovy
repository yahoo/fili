// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.CountAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchEstimatePostAggregation

import spock.lang.Specification

class TemplateDruidQuerySpec extends Specification {

    String arithmeticAggOperand1FieldName
    String arithmeticAggOperand1Name
    Aggregation arithmeticAggOperand1
    PostAggregation arithmeticPostAggOperand1

    String arithmeticAggOperand2FieldName
    String arithmeticAggOperand2Name
    Aggregation arithmeticAggOperand2
    PostAggregation arithmeticPostAggOperand2

    String arithmeticPostAggName
    ArithmeticPostAggregationFunction fn
    ArithmeticPostAggregation arithmeticPostAgg

    String thetaSketchPostAggName
    PostAggregation thetaSketchDependentPostAgg
    ThetaSketchEstimatePostAggregation thetaSketchPostAgg

    def setup() {
        arithmeticAggOperand1FieldName = "arithmetic_agg_operand_1"
        arithmeticAggOperand1Name = "arithmeticAggOperand1"
        arithmeticAggOperand1 = new LongSumAggregation(arithmeticAggOperand1Name, arithmeticAggOperand1FieldName)

        arithmeticAggOperand2FieldName = "arithmetic_agg_operand_2"
        arithmeticAggOperand2Name = "arithmeticAggOperand2"
        arithmeticAggOperand2 = new LongSumAggregation(arithmeticAggOperand2Name, arithmeticAggOperand2FieldName)

        arithmeticPostAggOperand1 = new FieldAccessorPostAggregation(arithmeticAggOperand1)

        arithmeticPostAggOperand2 = new FieldAccessorPostAggregation(arithmeticAggOperand2)

        arithmeticPostAggName = "arithmeticPostAgg"
        fn = ArithmeticPostAggregationFunction.PLUS
        arithmeticPostAgg = new ArithmeticPostAggregation(
                arithmeticPostAggName,
                fn,
                [arithmeticPostAggOperand1, arithmeticPostAggOperand2]
        )

        thetaSketchPostAggName = "thetaSketchPostAgg"
        thetaSketchDependentPostAgg = Mock(PostAggregation)
        thetaSketchPostAgg = new ThetaSketchEstimatePostAggregation(thetaSketchPostAggName, thetaSketchDependentPostAgg)
    }

    def "verify query.depth()"() {
        setup:
            Set<Aggregation> aggs = []
            Set<PostAggregation> postAggs = []

            TemplateDruidQuery q1 = new TemplateDruidQuery(aggs, postAggs)
            TemplateDruidQuery q2 = new TemplateDruidQuery(aggs, postAggs, q1)
            TemplateDruidQuery q3 = new TemplateDruidQuery(aggs, postAggs, q2)

        expect:
            q1.depth() == 1
            q2.depth() == 2
            q3.depth() == 3
    }

    def "verify query.isNested()"() {
        setup:
            Set<Aggregation> aggs = []
            Set<PostAggregation> postAggs = []

            TemplateDruidQuery q1 = new TemplateDruidQuery(aggs, postAggs)
            TemplateDruidQuery q2 = new TemplateDruidQuery(aggs, postAggs, q1)
            TemplateDruidQuery q3 = new TemplateDruidQuery(aggs, postAggs, q2)

        expect:
            q1.isNested() == false
            q2.isNested() == true
            q3.isNested() == true
            q2.getInnerQuery().get().isNested() == false
            q3.getInnerQuery().get().isNested() == true
            q3.getInnerQuery().get().getInnerQuery().get().isNested() == false
            q1.isTimeGrainValid() == true
    }

    def "verify query.nest()"() {
        setup:
            Aggregation agg1 = new LongSumAggregation("field1", "field1")
            Aggregation agg2 = new LongMaxAggregation("field2", "field2")
            PostAggregation postagg1 = new FieldAccessorPostAggregation(agg1)
            PostAggregation postagg2 = new FieldAccessorPostAggregation(agg2)
            PostAggregation postagg3 = new ArithmeticPostAggregation("field3", ArithmeticPostAggregationFunction.PLUS,
                    [postagg1, postagg2]
            )
            TemplateDruidQuery q1 = new TemplateDruidQuery([agg1,agg2] as Set, [postagg3] as Set)
            TemplateDruidQuery q2 = q1.nest()

        expect:
            q2.is(q1) == false //q2 should be a new object
            q2.getAggregations().sort() == [agg1, agg2].sort()
            q2.getPostAggregations().sort() == [postagg3].sort()
            q2.getInnerQuery() != null
            q2.getInnerQuery().get().getAggregations().sort() == [agg1, agg2].sort()
            q2.getInnerQuery().get().getPostAggregations().isEmpty()
    }

    def "verify q1.merge(q2) equals merged"() {
         setup:
            Aggregation q1_agg1 = new LongSumAggregation("field1", "field1")
            Aggregation q1_agg2 = new LongMaxAggregation("field2", "field2")
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

            TemplateDruidQuery nested = new TemplateDruidQuery([nested_agg1] as Set, [nested_postagg1] as Set)
            TemplateDruidQuery q1 = new TemplateDruidQuery([q1_agg1,q1_agg2] as Set, [q1_postagg1] as Set, nested)

            TemplateDruidQuery q2 = new TemplateDruidQuery([q2_agg1] as Set, [q2_postagg1] as Set)

            TemplateDruidQuery merged = q1.merge(q2)

         expect:
            //q2_agg1 is nested where the outer query is now (name="foo", fieldName="foo)
            merged.getAggregations().sort() == [q1_agg1, q1_agg2, new LongSumAggregation("foo", "foo")].sort()
            merged.getPostAggregations().sort() == [q1_postagg1, q2_postagg1].sort()
            merged.depth() == 2
            merged.getInnerQuery().get().getAggregations().sort() == [q2_agg1, nested_agg1].sort()
            merged.getInnerQuery().get().getPostAggregations().sort() == [nested_postagg1]

            q1.merge(q2) == q2.merge(q1)
    }

    def "verify q1.merge(q2) fails for duplicate aggregation names"() {
        setup:
            Aggregation q1_agg1 = new LongSumAggregation("field1", "field1")
            Aggregation q1_agg2 = new LongMaxAggregation("field2", "field2")
            Aggregation nested_agg1 = new DoubleSumAggregation("duplicate", "duplicate")

            Aggregation q2_agg1 = new LongSumAggregation("duplicate", "duplicate")

            TemplateDruidQuery nested = new TemplateDruidQuery([nested_agg1] as Set, [] as Set)
            TemplateDruidQuery q1 = new TemplateDruidQuery([q1_agg1, q1_agg2] as Set, [] as Set, nested)
            TemplateDruidQuery q2 = new TemplateDruidQuery([q2_agg1] as Set, [] as Set)

        when:
            q1.merge(q2)

        then:
            thrown(IllegalArgumentException)
    }

    def "verify q1.merge(q2) fails for duplicate post aggregation names"() {
        setup:
            Aggregation q1_agg1 = new LongSumAggregation("field1", "field1")
            Aggregation q1_agg2 = new LongMaxAggregation("field2", "field2")
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

            TemplateDruidQuery nested = new TemplateDruidQuery([nested_agg1] as Set, [nested_postagg1] as Set)
            TemplateDruidQuery q1 = new TemplateDruidQuery([q1_agg1, q1_agg2] as Set, [q1_postagg1] as Set, nested)
            TemplateDruidQuery q2 = new TemplateDruidQuery([q2_agg1] as Set, [q2_postagg1] as Set)

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
            new TemplateDruidQuery([a1,a2,a3] as Set, [p1,p2,p3] as Set)

        then:
            thrown(IllegalArgumentException)
    }

    def "renaming metric field to same name just returns the tdq unchanged"() {
        setup:
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        when:
        TemplateDruidQuery result = tdq.renameMetricField("test", "test")

        then:
        result == tdq
    }

    def "renaming metric field to or from null results in null pointer immediately being thrown"() {
        setup:
        String testName = "name"
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        when:
        tdq.renameMetricField(null, testName)

        then:
        thrown(NullPointerException)

        when:
        tdq.renameMetricField(testName, null)

        then:
        thrown(NullPointerException)
    }

    def "correctly renames output name of matching post aggs"() {
        setup:
        String newName = "newName"
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [arithmeticPostAgg])

        when:
        TemplateDruidQuery result = tdq.renameMetricField(arithmeticPostAggName, newName)

        then: "same amount of aggs and post aggs"
        result.getAggregations().size() == 0
        result.getPostAggregations().size() == 1

        and: "output name was renamed"
        ArithmeticPostAggregation resultPostAgg = result.getPostAggregations().iterator().next()
        resultPostAgg.getName() == newName

        and: "all other fields are untouched"
        resultPostAgg.getFn() == fn
        resultPostAgg.getFields() == [arithmeticPostAggOperand1, arithmeticPostAggOperand2] as List
    }

    def "renaming post agg operates on correct post agg, and unrelated post aggs are ignored"() {
        setup:
        String newName = "newName"
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [arithmeticPostAgg, thetaSketchPostAgg])

        when:
        TemplateDruidQuery result = tdq.renameMetricField(thetaSketchPostAggName, newName)

        then: "same amount of aggs and post aggs"
        result.getAggregations().size() == 0
        result.getPostAggregations().size() == 2

        and: "target post agg is correctly renamed"
        ThetaSketchEstimatePostAggregation resThetaSketch = result.getPostAggregations().find { it.getName() == newName }
        resThetaSketch != null
        resThetaSketch.getFields() == [thetaSketchDependentPostAgg]

        and: "unrelated post agg has not been touched"
        ArithmeticPostAggregation resUnrelated = result.getPostAggregations().find { it.getName() == arithmeticPostAggName }
        resUnrelated == arithmeticPostAgg
    }

    // This case SHOULD never come up. All field accessor post aggs should be dependencies of other post aggs, which
    // aren't traversed on output name rename
    def "when renaming OUTPUT post agg name, field access post aggs are ignored"() {
        setup:
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [arithmeticPostAggOperand1])

        when:
        tdq.renamePostAggregation(arithmeticAggOperand1Name, "unusedNewOutputName")

        then: "field accessor post agg with matching name was ignored"
        // Since field accessor post agg was ignored, AND template druid queries can't have post aggs with duplicate
        // output names, no post agg to rename is detected and an illegal argument exception is thrown
        thrown(IllegalArgumentException)
    }

    // This case SHOULD never come up. Under normal circumstances, this case would get detected as an aggregation rename
    // and the output rename method would never be called
    def "when renaming OUTPUT post agg name, dependent post aggs that match name are ignored"() {
        setup:
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [arithmeticPostAggOperand1])

        when:
        tdq.renamePostAggregation(arithmeticAggOperand1Name, "unusedNewOutputName")

        then: "dependent post agg with matching name was ignored"
        // Since dependent post aggs are not parsed and thus ignored no post with matching output name is detected and
        //  illegal argument exception is thrown
        thrown(IllegalArgumentException)
    }

    def "Long sum agg that is NOT referenced by a post agg is renamed successfully and unrelated aggs are untouched"() {
        setup:
        String newOutputName = "newOutputName"
        TemplateDruidQuery tdq = new TemplateDruidQuery([arithmeticAggOperand1, arithmeticAggOperand2], [])

        when:
        TemplateDruidQuery result = tdq.renameAggregation(arithmeticAggOperand1Name, newOutputName)

        then: "correct amount of aggs and post aggs in resultant tdq"
        result.getAggregations().size() == 2
        result.getPostAggregations().size() == 0

        and: "unrelated agg is untouched"
        arithmeticAggOperand2 == result.getAggregations().find { it.getName() == arithmeticAggOperand2Name }

        and: "renamed agg has output name changed, but field name is maintained"
        LongSumAggregation resAgg = result.getAggregations().find { it.getName() == newOutputName }
        resAgg.getFieldName() == arithmeticAggOperand1FieldName
    }

    def "Long sum agg that IS referenced is renamed, and references to it are renamed"() {
        setup:
        String newOutputName = "newOutputName"
        TemplateDruidQuery tdq = new TemplateDruidQuery(
                [arithmeticAggOperand1, arithmeticAggOperand2],
                [arithmeticPostAgg]
        )

        when:
        TemplateDruidQuery result = tdq.renameAggregation(arithmeticAggOperand1Name, newOutputName)

        then: "expected number of aggs and post aggs in result"
        result.getAggregations().size() == 2
        result.getPostAggregations().size() == 1

        and: "unrelated agg is untouched"
        arithmeticAggOperand2 == result.getAggregations().find { it.getName() == arithmeticAggOperand2Name }

        and: "Output name of top level post agg is untouched"
        ArithmeticPostAggregation resultPa = result.getPostAggregations().find { it.getName() == arithmeticPostAggName }
        resultPa != null

        and: "Unrelated dependent post agg is untouched"
        resultPa.getFields().find { FieldAccessorPostAggregation it -> it.getFieldName() == arithmeticAggOperand2Name }

        and: "target agg is renamed in both aggregations and post aggregations"
        Aggregation transformedAgg = result.getAggregations().find { it.getName() == newOutputName }
        transformedAgg != null
        transformedAgg.getFieldName() == arithmeticAggOperand1FieldName
        resultPa.getFields().find() { FieldAccessorPostAggregation it -> it.getMetricField() == transformedAgg } != null
    }

    def "If agg is referenced by multiple different post aggs, all references are renamed"() {
        setup:
        String arithmeticAggOperand3FieldName = "arithmetic_agg_operand_3"
        String arithmeticAggOperand3Name = "arithmeticAggOperand3"
        Aggregation arithmeticAggOperand3 = new LongSumAggregation(arithmeticAggOperand3Name, arithmeticAggOperand3FieldName)
        PostAggregation arithmeticPostAggOperand3 = new FieldAccessorPostAggregation(arithmeticAggOperand3)

        String arithmeticPostAgg2Name = "arithmeticPostAgg2"
        ArithmeticPostAggregationFunction fn2 = ArithmeticPostAggregationFunction.DIVIDE
        ArithmeticPostAggregation arithmeticPostAgg2 =  new ArithmeticPostAggregation(
                arithmeticPostAgg2Name,
                fn2,
                [arithmeticPostAggOperand2, arithmeticPostAggOperand3]
        )

        TemplateDruidQuery tdq = new TemplateDruidQuery(
                [arithmeticAggOperand1, arithmeticAggOperand2, arithmeticAggOperand3],
                [arithmeticPostAgg, arithmeticPostAgg2]
        )
        String newName = "newName"

        when:
        TemplateDruidQuery result = tdq.renameAggregation(arithmeticAggOperand2Name, newName)

        then: "target agg is renamed, other aggs are untouched"
        Aggregation resultAgg = result.getAggregations().find { it.getName() == newName }
        resultAgg.getFieldName() == arithmeticAggOperand2FieldName
        result.getAggregations().find { it.getName() == arithmeticAggOperand1Name } == arithmeticAggOperand1
        result.getAggregations().find { it.getName() == arithmeticAggOperand3Name} == arithmeticAggOperand3

        and: "field access in first arithmetic post agg is renamed, other parts of post agg are untouched"
        ArithmeticPostAggregation resPostAgg1 = result.getPostAggregations().find { it.getName() == arithmeticPostAggName }
        FieldAccessorPostAggregation renamedPostAgg = resPostAgg1.getFields().get(1)
        resPostAgg1.getFields().get(0) == arithmeticPostAggOperand1
        renamedPostAgg.getMetricField() == resultAgg

        and: "field access in second arithmetic agg is renamed, other parts of post agg are untouched"
        ArithmeticPostAggregation resPostAgg2 = result.getPostAggregations().find { it.getName() == arithmeticPostAgg2Name }
        resPostAgg2.getFields().get(0) == renamedPostAgg
        resPostAgg2.getFields().get(1) == arithmeticPostAggOperand3
    }

    def "renaming an aggregation that is referenced by a post aggregation correctly renames both pieces"() {
        setup:
        String newName = "newName"
        TemplateDruidQuery tdq = new TemplateDruidQuery([arithmeticAggOperand1, arithmeticAggOperand2], [arithmeticPostAgg])

        when:
        TemplateDruidQuery resultTdq = tdq.renameMetricField(arithmeticAggOperand2Name, newName)

        then: "agg is correctly renamed"
        Aggregation renamedAgg = resultTdq.getAggregations().find { it.getName() == newName }
        renamedAgg.getFieldName() == arithmeticAggOperand2FieldName

        and: "post agg that references it is correctly renamed"
        ArithmeticPostAggregation resultPostAgg = resultTdq.getPostAggregations().iterator().next()
        resultPostAgg.getName() == arithmeticPostAggName
        resultPostAgg.getFields().get(0) == arithmeticPostAggOperand1
        ((FieldAccessorPostAggregation) resultPostAgg.getFields().get(1)).getMetricField() == renamedAgg
    }

    def "renaming a post aggregation only renames the output name"() {
        setup:
        String newName = "newName"
        TemplateDruidQuery tdq = new TemplateDruidQuery([arithmeticAggOperand1, arithmeticAggOperand2], [arithmeticPostAgg])

        when:
        TemplateDruidQuery resultTdq = tdq.renameMetricField(arithmeticPostAggName, newName)
        ArithmeticPostAggregation resultPostAgg = resultTdq.getPostAggregations().iterator().next()

        then:
        resultTdq.getAggregations().containsAll([arithmeticAggOperand1, arithmeticAggOperand2])
        resultPostAgg.getFields() == [arithmeticPostAggOperand1, arithmeticPostAggOperand2] as List<PostAggregation>
        resultPostAgg.getName() == newName
    }

    def "attempting to rename a metric field that doesn't exist throws an error"() {
        setup:
        String nonExistantMetricName = "nonExistantMetric"
        String resultName = "unused"

        TemplateDruidQuery tdq = new TemplateDruidQuery([arithmeticAggOperand1, arithmeticAggOperand2], [arithmeticPostAgg])

        when:
        tdq.renameMetricField(nonExistantMetricName, resultName)

        then:
        // TODO extract message to commmon location
        IllegalArgumentException e = thrown(IllegalArgumentException)
        e.getMessage() == "no aggregation with name " + nonExistantMetricName + " exists."
    }

    def "If output name is already used throws an error"() {
        setup:
        TemplateDruidQuery tdq = new TemplateDruidQuery([arithmeticAggOperand1, arithmeticAggOperand2], [arithmeticPostAgg])

        when:
        tdq.renameMetricField(arithmeticAggOperand1Name, arithmeticAggOperand2Name)

        then:
        IllegalArgumentException e = thrown()
        // TODO extract message to common location
        e.getMessage() == "Can't rename " +
                arithmeticAggOperand1Name +
                " to " +
                arithmeticAggOperand2Name +
                " as that name already exists in this query"
    }

    // Tests
    // * test updating AggregationReference references
    // * test top level AggregationReference rename
    // TODO write "AggregationAliasingPostAgg", which is a post agg that aliases an Aggregation to a different output name
}
