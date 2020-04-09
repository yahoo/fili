// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric

import com.yahoo.bard.webservice.druid.druid.model.AggregationExternalNode
import com.yahoo.bard.webservice.druid.druid.model.PostAggregationExternalNode
import com.yahoo.bard.webservice.druid.druid.model.WithMetricFieldInternalNode
import com.yahoo.bard.webservice.druid.druid.model.WithPostAggsInternalNode
import com.yahoo.bard.webservice.druid.model.MetricField
import com.yahoo.bard.webservice.druid.model.WithMetricField
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.CountAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongMaxAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAliasingPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchEstimatePostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.WithPostAggregations

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
        PostAggregation postagg3 = new ArithmeticPostAggregation(
                "field3", ArithmeticPostAggregationFunction.PLUS,
                [postagg1, postagg2]
        )
        TemplateDruidQuery q1 = new TemplateDruidQuery([agg1, agg2] as Set, [postagg3] as Set)
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

        PostAggregation q1_postagg1 = new ArithmeticPostAggregation(
                "field5", ArithmeticPostAggregationFunction.PLUS,
                [
                        new FieldAccessorPostAggregation(q1_agg1),
                        new FieldAccessorPostAggregation(q1_agg2)
                ]
        )

        PostAggregation nested_postagg1 = new ConstantPostAggregation("field6", 100)
        PostAggregation q2_postagg1 = new ConstantPostAggregation("field7", 100)

        TemplateDruidQuery nested = new TemplateDruidQuery([nested_agg1] as Set, [nested_postagg1] as Set)
        TemplateDruidQuery q1 = new TemplateDruidQuery([q1_agg1, q1_agg2] as Set, [q1_postagg1] as Set, nested)

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

        PostAggregation q1_postagg1 = new ArithmeticPostAggregation(
                "duplicate", ArithmeticPostAggregationFunction.PLUS,
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
        new TemplateDruidQuery([a1, a2, a3] as Set, [p1, p2, p3] as Set)

        then:
        thrown(IllegalArgumentException)
    }

    // Test tree iteration that backs renaming
    // Tests written:
    //  - base case 1: field to check is external node but does NOT match
    //  - base case 2: field to check is field to be replaced. Just return new field
    //  - complex base case: field to check HAS children but IS node to be remapped. New node is returned, children are NOT parsed
    //  - internal node 1: MetricFieldReference whose child is the node to replace.  Node is rebuilt, repointed to new node.
    //  - internal node 2: PostAggReference whose CHILD is the node to replace. Node is returned with repoint child and other children intact
    //  - internal node 3: MetricFieldReference whose child is NOT the node to replace. Node is rebuilt but structure is same.
    //  - complex internal node: Post agg reference whose child has a child that is the node to replace. Node is successfully swapped and tree is otherwise maintained
    
    def "Checking external node that is NOT the target node simply returns input node"() {
        given:
        MetricField toReplace = Mock()
        MetricField replacement = Mock()
        MetricField checkedNode = Mock()
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        expect:
        tdq.repointToNewMetricField(toReplace, replacement, checkedNode) == checkedNode
    }

    def "Checking external node that IS the target node returns node to replace target with (result node)"() {
        given:
        MetricField toReplace = Mock()
        MetricField replacement = Mock()
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        expect:
        tdq.repointToNewMetricField(toReplace, replacement, toReplace) == replacement
    }

    def "Checking internal node that IS the target node returns the result node WITHOUT checking subtree"() {
        setup:
        WithPostAggregations<PostAggregation> toReplace1 = Mock()
        WithPostAggregations<PostAggregation> replacement1 = Mock()

        WithMetricField toReplace2 = Mock()
        WithMetricField replacement2 = Mock()

        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        when:
        WithPostAggregations<PostAggregation> res = tdq.repointToNewMetricField(toReplace1, replacement1, toReplace1)

        then:
        res == replacement1
        0 * toReplace1.getPostAggregations()

        when:
        WithMetricField res2 = tdq.repointToNewMetricField(toReplace2, replacement2, toReplace2)

        then:
        res2 == replacement2
        0 * toReplace2.getMetricField()
    }

    def "Internal whose single child is the node to replace is updated properly"() {
        given:
        MetricField toReplace = new AggregationExternalNode()
        MetricField replacement = new AggregationExternalNode()
        WithMetricFieldInternalNode parent = new WithMetricFieldInternalNode(toReplace)
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        expect:
        tdq.repointToNewMetricField(toReplace, replacement, parent).getMetricField() == replacement
    }

    def "When rebuilding sub tree order of children is maintained, node to replace is properly replaced"() {
        PostAggregation toReplace = new PostAggregationExternalNode()
        PostAggregation replacement = new PostAggregationExternalNode()
        PostAggregation otherField1 = new PostAggregationExternalNode()
        PostAggregation otherField2 = new PostAggregationExternalNode()
        PostAggregation otherField3 = new PostAggregationExternalNode()
        List<PostAggregation> children = [otherField1, otherField2, toReplace, otherField3] as List
        WithPostAggsInternalNode root = new WithPostAggsInternalNode(children)
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        when:
        WithPostAggsInternalNode result = tdq.repointToNewMetricField(toReplace, replacement, root)

        then:
        result.getPostAggregations() == [otherField1, otherField2, replacement, otherField3]
    }

    def "Tree is properly rebuilt even if nothing is replaced"() {
        PostAggregation field1 = new PostAggregationExternalNode()
        PostAggregation field2 = new PostAggregationExternalNode()
        PostAggregation field3 = new PostAggregationExternalNode()
        List<PostAggregation> children = [field1, field2, field3] as List
        WithPostAggsInternalNode root = new WithPostAggsInternalNode(children)
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        when:
        WithPostAggsInternalNode result = tdq.repointToNewMetricField(Mock(MetricField), Mock(MetricField), root)

        then:
        result.getPostAggregations() == children
    }

    /**
     * Test tree structure. Structure is top down, numbering is left to right
     * i = internal node
     * a = aggregation external node
     * p = post aggregation
     * r = node to replace
     *
     * l1:            i
     *            /    \  \
     * l2:      i      i   p
     *        /  \     |
     * l3:   p   r     a
     *         /  \
     * l4:    p    p
     */
    def "Deep replacement occurs properly"() {
        setup:
        // l4
        PostAggregation l4Ext1 = new PostAggregationExternalNode()
        PostAggregation l4Ext2 = new PostAggregationExternalNode()
        List<PostAggregation> toReplaceChildren = [l4Ext1, l4Ext2] as List

        // l3
        PostAggregation l3Ext1 = new PostAggregationExternalNode()
        WithPostAggsInternalNode toReplace = new WithPostAggsInternalNode(toReplaceChildren)
        Aggregation l3Ext2 = new AggregationExternalNode()
        List<PostAggregation> l2Int1Children =[l3Ext1, toReplace]

        // l2
        WithPostAggsInternalNode l2Int1 = new WithPostAggsInternalNode(l2Int1Children)
        WithMetricFieldInternalNode l2Int2 = new WithMetricFieldInternalNode(l3Ext2)
        PostAggregation l2Ext3 = new PostAggregationExternalNode()
        List<PostAggregation> rootChildren = [l2Int1, l2Int2, l2Ext3]

        // l1
        WithPostAggsInternalNode root = new WithPostAggsInternalNode(rootChildren)

        PostAggregationExternalNode replacement = new PostAggregationExternalNode()
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        when:
        WithPostAggsInternalNode result = tdq.repointToNewMetricField(toReplace, replacement, root)

        then: "l2 branches 2 and 3 are exactly maintained"
        List<PostAggregation> resultChildren = result.getPostAggregations()
        resultChildren.get(1) == l2Int2
        resultChildren.get(2) == l2Ext3

        and: "l3 branch 1 is maintained, branch 2 was swapped"
        // l3 branch 3 already checked by ensuring unrelated branches off root are maintained
        WithPostAggsInternalNode l2MutatedSubtree = resultChildren.get(0)
        List<PostAggregation> l2MutatedSubtreeChildren = l2MutatedSubtree.getPostAggregations()
        l2MutatedSubtreeChildren.size() == 2
        l2MutatedSubtreeChildren.get(0) == l3Ext1
        l2MutatedSubtreeChildren.get(1) == replacement
    }

    // Test renaming correctly mutates relevant fields
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

    def "attempting to rename a metric field that doesn't exist throws an error"() {
        setup:
        String nonExistantMetricName = "nonExistantMetric"
        String resultName = "unused"

        TemplateDruidQuery tdq = new TemplateDruidQuery(
                [arithmeticAggOperand1, arithmeticAggOperand2],
                [arithmeticPostAgg]
        )

        when:
        tdq.renameMetricField(nonExistantMetricName, resultName)

        then:
        IllegalArgumentException e = thrown(IllegalArgumentException)
        e.getMessage() == String.
                format(TemplateDruidQuery.NO_METRIC_TO_RENAME_FOUND_ERROR_MESSAGE, nonExistantMetricName)
    }

    def "Attempting to rename to a name that is already in use throws an error"() {
        setup:
        TemplateDruidQuery tdq = new TemplateDruidQuery(
                [arithmeticAggOperand1, arithmeticAggOperand2],
                [arithmeticPostAgg]
        )

        when:
        tdq.renameMetricField(arithmeticAggOperand1Name, arithmeticAggOperand2Name)

        then:
        IllegalArgumentException e = thrown()
        e.getMessage() == String.format(
                TemplateDruidQuery.RENAME_TO_DUPLICATE_NAME_ERROR_MESSAGE,
                arithmeticAggOperand1Name,
                arithmeticAggOperand2Name
        )
    }

    def "Single agg is renamed correctly, other aggs are untouched"() {
        setup:
        String newOutputName = "newOutputName"
        TemplateDruidQuery tdq = new TemplateDruidQuery([arithmeticAggOperand1, arithmeticAggOperand2], [])

        when:
        TemplateDruidQuery result = tdq.renameMetricField(arithmeticAggOperand1Name, newOutputName)

        then: "correct amount of aggs and post aggs in resultant tdq"
        result.getAggregations().size() == 2
        result.getPostAggregations().size() == 0

        and: "unrelated agg is untouched"
        arithmeticAggOperand2 == result.getAggregations().find { it.getName() == arithmeticAggOperand2Name }

        and: "renamed agg has output name changed, but field name is maintained"
        LongSumAggregation resAgg = result.getAggregations().find { it.getName() == newOutputName }
        resAgg.getFieldName() == arithmeticAggOperand1FieldName
    }

    def "Single post agg is renamed correctly"() {
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
        resultPostAgg.getPostAggregations() == [arithmeticPostAggOperand1, arithmeticPostAggOperand2] as List
    }

    def "Renaming single field does not affect other fields in either agg nor post agg set"() {
        setup:
        String newName = "newName"
        Set<Aggregation> aggs = [arithmeticAggOperand1, arithmeticAggOperand2]
        TemplateDruidQuery tdq = new TemplateDruidQuery(aggs, [arithmeticPostAgg, thetaSketchPostAgg])

        when:
        TemplateDruidQuery result = tdq.renameMetricField(thetaSketchPostAggName, newName)

        then: "same amount of aggs and post aggs"
        result.getAggregations().size() == 2
        result.getPostAggregations().size() == 2

        and: " aggregations are untouched"
        result.getAggregations() == aggs

        and: "target post agg is correctly renamed"
        ThetaSketchEstimatePostAggregation resThetaSketch = result.
                getPostAggregations().
                find { it.getName() == newName }
        resThetaSketch != null
        resThetaSketch.getPostAggregations() == [thetaSketchDependentPostAgg]

        and: "unrelated post agg has not been touched"
        ArithmeticPostAggregation resUnrelated = result.
                getPostAggregations().
                find { it.getName() == arithmeticPostAggName }
        resUnrelated == arithmeticPostAgg
    }

    // TODO examine these methods
    // Tested so far:
    //  - input name and output name are equivalent is noop
    //  - error cases
    //    - can't rename to or from null
    //    - can't rename field that doesn't exist
    //    - can't rename field to name that is already in use
    //  - Renaming single aggregation works properly, other aggs untouched
    //  - Renaming single post aggregation works properly
    //  - Renaming single post aggregation does not affect unrelated post aggs nor aggs

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
        resultPa.getPostAggregations().find { FieldAccessorPostAggregation it -> it.getFieldName() == arithmeticAggOperand2Name }

        and: "target agg is renamed in both aggregations and post aggregations"
        Aggregation transformedAgg = result.getAggregations().find { it.getName() == newOutputName }
        transformedAgg != null
        transformedAgg.getFieldName() == arithmeticAggOperand1FieldName
        resultPa.getPostAggregations().find() { FieldAccessorPostAggregation it -> it.getMetricField() == transformedAgg } != null
    }

    def "If agg is referenced by multiple different post aggs, all references are renamed"() {
        setup:
        String arithmeticAggOperand3FieldName = "arithmetic_agg_operand_3"
        String arithmeticAggOperand3Name = "arithmeticAggOperand3"
        Aggregation arithmeticAggOperand3 = new LongSumAggregation(
                arithmeticAggOperand3Name,
                arithmeticAggOperand3FieldName
        )
        PostAggregation arithmeticPostAggOperand3 = new FieldAccessorPostAggregation(arithmeticAggOperand3)

        String arithmeticPostAgg2Name = "arithmeticPostAgg2"
        ArithmeticPostAggregationFunction fn2 = ArithmeticPostAggregationFunction.DIVIDE
        ArithmeticPostAggregation arithmeticPostAgg2 = new ArithmeticPostAggregation(
                arithmeticPostAgg2Name,
                fn2,
                [arithmeticPostAggOperand2, arithmeticPostAggOperand3]
        )

        String aggregationAliasingPostAggName = "arithmeticAggOperand2Alias"
        FieldAliasingPostAggregation aggregationAliasingPostAgg = new FieldAliasingPostAggregation(
                aggregationAliasingPostAggName,
                arithmeticAggOperand2
        )

        TemplateDruidQuery tdq = new TemplateDruidQuery(
                [arithmeticAggOperand1, arithmeticAggOperand2, arithmeticAggOperand3],
                [arithmeticPostAgg, arithmeticPostAgg2, aggregationAliasingPostAgg]
        )
        String newName = "newName"

        when:
        TemplateDruidQuery result = tdq.renameAggregation(arithmeticAggOperand2Name, newName)

        then: "target agg is renamed, other aggs are untouched"
        Aggregation resultAgg = result.getAggregations().find { it.getName() == newName }
        resultAgg.getFieldName() == arithmeticAggOperand2FieldName
        result.getAggregations().find { it.getName() == arithmeticAggOperand1Name } == arithmeticAggOperand1
        result.getAggregations().find { it.getName() == arithmeticAggOperand3Name } == arithmeticAggOperand3

        and: "field access in first arithmetic post agg is renamed, other parts of post agg are untouched"
        ArithmeticPostAggregation resPostAgg1 = result.getPostAggregations().find { it.getName() == arithmeticPostAggName }
        FieldAccessorPostAggregation renamedPostAgg = resPostAgg1.getPostAggregations().get(1)
        resPostAgg1.getPostAggregations().get(0) == arithmeticPostAggOperand1
        renamedPostAgg.getMetricField() == resultAgg

        and: "field access in second arithmetic agg is renamed, other parts of post agg are untouched"
        ArithmeticPostAggregation resPostAgg2 = result.getPostAggregations().find { it.getName() == arithmeticPostAgg2Name }
        resPostAgg2.getPostAggregations().get(0) == renamedPostAgg
        resPostAgg2.getPostAggregations().get(1) == arithmeticPostAggOperand3

        and: "top level aggregation referencing post agg is renamed"
        FieldAliasingPostAggregation resAliasingPostAgg = result.getPostAggregations().find { it.getName() == aggregationAliasingPostAggName }
        resAliasingPostAgg.getAggregations().get(0) == resultAgg
    }

    def "renaming an aggregation that is referenced by a post aggregation correctly renames both pieces"() {
        setup:
        String newName = "newName"
        TemplateDruidQuery tdq = new TemplateDruidQuery(
                [arithmeticAggOperand1, arithmeticAggOperand2],
                [arithmeticPostAgg]
        )

        when:
        TemplateDruidQuery resultTdq = tdq.renameMetricField(arithmeticAggOperand2Name, newName)

        then: "agg is correctly renamed"
        Aggregation renamedAgg = resultTdq.getAggregations().find { it.getName() == newName }
        renamedAgg.getFieldName() == arithmeticAggOperand2FieldName

        and: "post agg that references it is correctly renamed"
        ArithmeticPostAggregation resultPostAgg = resultTdq.getPostAggregations().iterator().next()
        resultPostAgg.getName() == arithmeticPostAggName
        resultPostAgg.getPostAggregations().get(0) == arithmeticPostAggOperand1
        ((FieldAccessorPostAggregation) resultPostAgg.getPostAggregations().get(1)).getMetricField() == renamedAgg
    }

    def "renaming a post aggregation only renames the output name"() {
        setup:
        String newName = "newName"
        TemplateDruidQuery tdq = new TemplateDruidQuery(
                [arithmeticAggOperand1, arithmeticAggOperand2],
                [arithmeticPostAgg]
        )

        when:
        TemplateDruidQuery resultTdq = tdq.renameMetricField(arithmeticPostAggName, newName)
        ArithmeticPostAggregation resultPostAgg = resultTdq.getPostAggregations().iterator().next()

        then:
        resultTdq.getAggregations().containsAll([arithmeticAggOperand1, arithmeticAggOperand2])
        resultPostAgg.getPostAggregations() == [arithmeticPostAggOperand1, arithmeticPostAggOperand2] as List<PostAggregation>
        resultPostAgg.getName() == newName
    }

    def "Renaming AggregationReferences updates relevant reference to new Aggregation"() {
        setup:
        String newName = "newName"
        LongSumAggregation updatedAgg = new LongSumAggregation(newName, arithmeticAggOperand1FieldName)
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        when:
        FieldAccessorPostAggregation result = tdq.renameAggregationReference(
                arithmeticAggOperand1Name,
                updatedAgg,
                (FieldAccessorPostAggregation) arithmeticPostAggOperand1
        )

        then:
        result.getMetricField() == updatedAgg
    }

    def "Renaming AggregationReference with aggregation that it does NOT depend on returns a copy of the AggregationReference"() {
        setup:
        String newName = "newName"
        LongSumAggregation updatedAgg = new LongSumAggregation(newName, arithmeticAggOperand1FieldName)
        TemplateDruidQuery tdq = new TemplateDruidQuery([], [])

        when:
        FieldAccessorPostAggregation result = tdq.renameAggregationReference(
                arithmeticAggOperand2Name,
                updatedAgg,
                (FieldAccessorPostAggregation) arithmeticPostAggOperand1
        )

        then:
        result.getMetricField() == arithmeticAggOperand1
    }

    // TODO test renaming target of filtered aggregator
    // TODO test post agg that references other post agg
}

import spock.lang.Specification
