// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.druid.model;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import java.util.Collections;
import java.util.Set;

/**
 * Aggregations and Post Aggregations in a druid query can be viewed as a tree, where internal nodes are MetricFields
 * that reference other MetricFields, and external nodes are MetricFields that directly reference a druid column. This
 * is a test class that explicitly exposes this tree behavior without any other backend constraints. This class
 * represents an external node of type PostAggregation.
 * <p>
 * The {@link WithPostAggsInternalNode}, {@link WithMetricFieldInternalNode}, and {@link AggregationExternalNode}
 * classes represent other node types in this tree structure.
 */
public class PostAggregationExternalNode extends PostAggregation {

    /**
     * Name used by all instances of this class.
     */
    public static final String NAME = "defaultName";

    /**
     * Constructor.
     */
    public PostAggregationExternalNode() {
        super(() -> "", NAME);
    }

    /**
     * This class doesn't track name, just return new instance.
     */
    @Override
    public PostAggregationExternalNode withName(String name) {
        return new PostAggregationExternalNode();
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
    public Set<Dimension> getDependentDimensions() {
        return Collections.emptySet();
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        // java hash code as there is no state that should be hashed on
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "PostAggregationExternalNode{" +
                "id=" + Integer.toHexString(System.identityHashCode(this)) +
                "}";
    }
}
