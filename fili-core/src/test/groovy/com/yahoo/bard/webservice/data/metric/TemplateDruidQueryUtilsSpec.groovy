// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric

import com.yahoo.bard.webservice.druid.druid.model.AggregationExternalNode
import com.yahoo.bard.webservice.druid.druid.model.PostAggregationExternalNode
import com.yahoo.bard.webservice.druid.druid.model.WithMetricFieldInternalNode
import com.yahoo.bard.webservice.druid.druid.model.WithPostAggsInternalNode
import com.yahoo.bard.webservice.druid.model.MetricField
import com.yahoo.bard.webservice.druid.model.WithMetricField
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.WithPostAggregations

import spock.lang.Specification

class TemplateDruidQueryUtilsSpec extends Specification {

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

        expect:
        TemplateDruidQueryUtils.repointToNewMetricField(checkedNode, toReplace, replacement) == checkedNode
    }

    def "Checking external node that IS the target node returns node to replace target with (result node)"() {
        given:
        MetricField toReplace = Mock()
        MetricField replacement = Mock()

        expect:
        TemplateDruidQueryUtils.repointToNewMetricField(toReplace, toReplace, replacement) == replacement
    }

    def "Checking internal node that IS the target node returns the result node WITHOUT checking subtree"() {
        setup:
        WithPostAggregations<PostAggregation> toReplace1 = Mock()
        WithPostAggregations<PostAggregation> replacement1 = Mock()

        WithMetricField toReplace2 = Mock()
        WithMetricField replacement2 = Mock()


        when:
        WithPostAggregations<PostAggregation> res = TemplateDruidQueryUtils.repointToNewMetricField(toReplace1, toReplace1, replacement1,)

        then:
        res == replacement1
        0 * toReplace1.getPostAggregations()

        when:
        WithMetricField res2 = TemplateDruidQueryUtils.repointToNewMetricField(toReplace2, toReplace2, replacement2)

        then:
        res2 == replacement2
        0 * toReplace2.getMetricField()
    }

    def "Internal whose single child is the node to replace is updated properly"() {
        given:
        MetricField toReplace = new AggregationExternalNode()
        MetricField replacement = new AggregationExternalNode()
        WithMetricFieldInternalNode parent = new WithMetricFieldInternalNode(toReplace)

        expect:
        TemplateDruidQueryUtils.repointToNewMetricField(parent, toReplace, replacement).getMetricField() == replacement
    }

    def "When rebuilding sub tree order of children is maintained, node to replace is properly replaced"() {
        PostAggregation toReplace = new PostAggregationExternalNode()
        PostAggregation replacement = new PostAggregationExternalNode()
        PostAggregation otherField1 = new PostAggregationExternalNode()
        PostAggregation otherField2 = new PostAggregationExternalNode()
        PostAggregation otherField3 = new PostAggregationExternalNode()
        List<PostAggregation> children = [otherField1, otherField2, toReplace, otherField3] as List
        WithPostAggsInternalNode root = new WithPostAggsInternalNode(children)

        when:
        WithPostAggsInternalNode result = TemplateDruidQueryUtils.repointToNewMetricField(root, toReplace, replacement)

        then:
        result.getPostAggregations() == [otherField1, otherField2, replacement, otherField3]
    }

    def "Tree is properly rebuilt even if nothing is replaced"() {
        PostAggregation field1 = new PostAggregationExternalNode()
        PostAggregation field2 = new PostAggregationExternalNode()
        PostAggregation field3 = new PostAggregationExternalNode()
        List<PostAggregation> children = [field1, field2, field3] as List
        WithPostAggsInternalNode root = new WithPostAggsInternalNode(children)

        when:
        WithPostAggsInternalNode result = TemplateDruidQueryUtils.repointToNewMetricField(root, Mock(MetricField), Mock(MetricField))

        then:
        result.getPostAggregations() == children
    }

    /**
     * Test tree structure. Structure is top down, numbering is left to right
     * i = internal node
     * a = aggregation external node
     * p = post aggregation external node
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

        when:
        WithPostAggsInternalNode result = TemplateDruidQueryUtils.repointToNewMetricField(root, toReplace, replacement)

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
}
