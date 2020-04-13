// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.druid.model;

import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.WithPostAggregations;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregations and Post Aggregations in a druid query can be viewed as a tree, where internal nodes are MetricFields
 * that reference other MetricFields, and external nodes are MetricFields that directly reference a druid column. This
 * is a test class that explicitly exposes this tree behavior without any other backend constraints. This class
 * represents an internal node that can have 0 or more children.
 * <p>
 * While the children of this class ARE mutable, defensive copies are used for exposing state. To mutate this class
 * clients must use mutator methods, no references to internal state are exposed.
 * <p>
 * The {@link WithMetricFieldInternalNode}, {@link AggregationExternalNode}, and {@link PostAggregationExternalNode}
 * classes represent other node types in this tree structure.
 */
public class WithPostAggsInternalNode extends PostAggregation implements WithPostAggregations {

    /**
     * Name used for all instances of this node type.
     */
    public static final String NAME = "name";

    private List<PostAggregation> children;

    /**
     * Constructor.
     *
     * @param children  The children of this node.
     */
    public WithPostAggsInternalNode(List<PostAggregation> children) {
        super(() -> "", NAME);
        this.children = new ArrayList<>(children);
    }

    /**
     * Sets children to copy of internal list. Mutates the instance this is called on.
     *
     * @param children  New children PostAggregations
     */
    public void setChildren(List<PostAggregation> children) {
        this.children = new ArrayList<>(children);
    }

    @Override
    public List<PostAggregation> getPostAggregations() {
        return new ArrayList<>(children);
    }

    /**
     * NOT a mutator method. As Interface Javadocs state, this returns a COPY that references the provided fields.
     *
     * @inheritDocs
     */
    @Override
    public WithPostAggregations withPostAggregations(List<? extends PostAggregation> fields) {
        return new WithPostAggsInternalNode(new ArrayList<>(fields));
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * This class doesn't track name, so just returns a copy.
     */
    @Override
    public WithPostAggsInternalNode withName(String name) {
        return new WithPostAggsInternalNode(children);
    }

    @Override
    public boolean isSketch() {
        return false;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (! (o instanceof WithPostAggsInternalNode)) {
            return false;
        }
        WithPostAggsInternalNode that = (WithPostAggsInternalNode) o;
        return java.util.Objects.equals(this.children, that.children);
    }

    @Override
    public int hashCode() {
        // java hash code as there is no state that should be hashed on
        return children.hashCode();
    }

    // TODO core state is children auxiliary state is else
    @Override
    public String toString() {
        return "WithMetricFieldInternalNode{" +
                "children=" + children +
                "}";
    }
}
