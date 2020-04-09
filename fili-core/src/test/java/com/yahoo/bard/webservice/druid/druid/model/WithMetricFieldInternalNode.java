package com.yahoo.bard.webservice.druid.druid.model;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.WithMetricField;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.WithPostAggregations;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Aggregations and Post Aggregations in a druid query can be viewed as a tree, where internal nodse are MetricFields
 * that reference other MetricFields, and external nodes are MetricFields that directly reference a druid column. This
 * is a test class that explicitly exposes this tree behavior without any other backend constraints.
 * <p>
 * This class is mutable through the exposed mutator methods. Auxiliary state to support MetricField metadata that does
 * not directly interact with the tree model is exposed through mutable fields.
 *
 *
 * // TODO link WithMetricFieldInternalNode and MetricFieldExternalNode classes
 */
public class WithMetricFieldInternalNode extends PostAggregation implements WithMetricField {

    public static final String NAME = "name";

    private MetricField child;

    public WithMetricFieldInternalNode(MetricField child) {
        super(() -> "", NAME);
        this.child = child;
    }

    /**
     * Sets children to copy of internal list. Mutates the instance this is called on.
     *
     * @param children  New children PostAggregations
     */
    public void setChild(MetricField child) {
        this.child = child;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * This class doesn't track name, so just returns a copy
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
