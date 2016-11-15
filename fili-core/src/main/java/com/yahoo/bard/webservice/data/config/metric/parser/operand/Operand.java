// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

/**
 * An operand is used as an argument to an Operator
 *
 * This interface seems silly. Surely there's a better way.
 */
public interface Operand {

    /**
     * Get this object as a FilterNode, or throw an exception.
     *
     * @return the filter node
     */
    default FilterNode getFilterNode() {
        if (FilterNode.class.isAssignableFrom(this.getClass())) {
            return (FilterNode) this;
        }

        throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    /**
     * Get this object as a MetricNode, or throw an exception.
     *
     * @return the metric node
     */
    default MetricNode getMetricNode() {
        if (MetricNode.class.isAssignableFrom(this.getClass())) {
            return (MetricNode) this;
        }

        throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    /**
     * Get this object as a ConstantMetricNode, or throw an exception.
     *
     * @return the constant node
     */
    default ConstantMetricNode getConstantNode() {
        if (ConstantMetricNode.class.isAssignableFrom(this.getClass())) {
            return (ConstantMetricNode) this;
        }

        throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    /**
     * Get this object as an IdentifierNode, or throw an exception.
     *
     * @return the identifier node
     */
    default IdentifierNode getIdentifierNode() {
        if (IdentifierNode.class.isAssignableFrom(this.getClass())) {
            return (IdentifierNode) this;
        }

        throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    /**
     * Get this object as a DimensionNode, or throw an exception.
     *
     * @return the dimension node
     */
    default DimensionNode getDimensionNode() {
        if (DimensionNode.class.isAssignableFrom(this.getClass())) {
            return (DimensionNode) this;
        }

        throw new UnsupportedOperationException("Operation not yet implemented.");
    }
}
