// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.druid.model;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Collections;
import java.util.Set;

/**
 * Aggregations and Post Aggregations in a druid query can be viewed as a tree, where internal nodes are MetricFields
 * that reference other MetricFields, and external nodes are MetricFields that directly reference a druid column. This
 * is a test class that explicitly exposes this tree behavior without any other backend constraints. This class
 * represents an external node of type Aggregation.
 * <p>
 * The {@link WithPostAggsInternalNode}, {@link WithMetricFieldInternalNode}, and {@link PostAggregationExternalNode}
 * classes represent other node types in this tree structure.
 */
public class AggregationExternalNode extends Aggregation {

    /**
     * Name used for all instances of this class.
     */
    public static final String NAME = "defaultName";

    /**
     * Field name used for all instances of this class.
     */
    public static final String FIELD_NAME = "defaultFieldName";

    /**
     * Constructor.
     */
    public AggregationExternalNode() {
        super(NAME, FIELD_NAME);
    }

    @Override
    public String getType() {
        return null;
    }

    /**
     * This class doesn't track name, so just returns a new instance.
     */
    @Override
    public Aggregation withName(String name) {
        return new AggregationExternalNode();
    }

    /**
     * This class doesn't track field name, so just returns a new instance.
     */
    @Override
    public Aggregation withFieldName(final String fieldName) {
        return new AggregationExternalNode();
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
        return "AggregationExternalNode{" +
                "id=" + Integer.toHexString(System.identityHashCode(this)) +
                "}";
    }
}
