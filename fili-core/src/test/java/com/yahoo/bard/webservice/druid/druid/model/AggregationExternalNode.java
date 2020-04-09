package com.yahoo.bard.webservice.druid.druid.model;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * // TODO update docs
 * Aggregations and Post Aggregations in a druid query can be viewed as a tree, where internal nodse are MetricFields
 * that reference other MetricFields, and external nodes are MetricFields that directly reference a druid column. This
 * is a test class that explicitly exposes this tree behavior without any other backend constraints.
 * <p>
 * While the children of this class ARE mutable, defensive copies are used for exposing state. To mutate this class
 * clients must use mutator methods, no references to internal state are exposed. Auxiliary state to support
 * MetricField metadata that does not directly interact with the tree model is exposed through mutable fields.
 *
 * // TODO link WithMetricFieldInternalNode and MetricFieldExternalNode classes
 */
public class AggregationExternalNode extends Aggregation {

    public static final String NAME = "defaultName";
    public static final String FIELD_NAME = "defaultFieldName";

    public AggregationExternalNode() {
        super(NAME, FIELD_NAME);
    }

    @Override
    public String getType() {
        return null;
    }

    /**
     * This class doesn't track name, so just returns a new instance
     */
    @Override
    public Aggregation withName(String name) {
        return new AggregationExternalNode();
    }

    /**
     * This class doesn't track field name, so just returns a new instance
     */
    @Override
    public Aggregation withFieldName(final String fieldName) {
        return new AggregationExternalNode();
    }

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
