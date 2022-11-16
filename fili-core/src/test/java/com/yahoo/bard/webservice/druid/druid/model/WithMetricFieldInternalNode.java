// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.druid.model;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.WithMetricField;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

/**
 * Aggregations and Post Aggregations in a druid query can be viewed as a tree, where internal nodes are MetricFields
 * that reference other MetricFields, and external nodes are MetricFields that directly reference a druid column. This
 * is a test class that explicitly exposes this tree behavior without any other backend constraints. This class
 * represents an internal node that has exactly 1 child.
 * <p>
 * While the child of this class is mutable, defensive copies are used for exposing state. To mutate this class clients
 * must use mutator methods, no references to internal state are exposed.
 * <p>
 * The {@link WithPostAggsInternalNode}, {@link AggregationExternalNode}, and {@link PostAggregationExternalNode}
 * classes represent other node types in this tree structure.
 */
public class WithMetricFieldInternalNode extends PostAggregation implements WithMetricField {

    /**
     * Name used for all instances of this class.
     */
    public static final String NAME = "name";

    private MetricField child;

    /**
     * Constructor.
     *
     * @param child MetricField this depends on
     */
    public WithMetricFieldInternalNode(MetricField child) {
        super(() -> "", NAME);
        this.child = child;
    }

    /**
     * Sets child metric field to the parameter value. Mutates the instance this is called on.
     *
     * @param child  New child MetricField
     */
    public void setChild(MetricField child) {
        this.child = child;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * This class doesn't track name, so just returns a copy.
     */
    @Override
    public PostAggregation withName(String name) {
        return new WithMetricFieldInternalNode(child);
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
    public MetricField getMetricField() {
        return child;
    }

    @Override
    public WithMetricField withMetricField(MetricField field) {
        return new WithMetricFieldInternalNode(field);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (! (o instanceof WithMetricFieldInternalNode)) {
            return false;
        }
        WithMetricFieldInternalNode that = (WithMetricFieldInternalNode) o;
        return java.util.Objects.equals(this.child, that.child);
    }

    @Override
    public int hashCode() {
        // java hash code as there is no state that should be hashed on
        return child.hashCode();
    }

    // TODO core state is children auxiliary state is else
    @Override
    public String toString() {
        return "WithMetricFieldInternalNode{" +
                "child=" + child +
                "}";
    }
}
