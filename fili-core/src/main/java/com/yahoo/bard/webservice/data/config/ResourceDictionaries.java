// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

/**
 * Hold the resource dictionaries.
 */
public class ResourceDictionaries {

    public final PhysicalTableDictionary physical;
    public final LogicalTableDictionary logical;
    public final MetricDictionary metric;
    public final DimensionDictionary dimension;

    /**
     * Constructor.
     */
    public ResourceDictionaries() {
        physical = new PhysicalTableDictionary();
        logical = new LogicalTableDictionary();
        metric = new MetricDictionary();
        dimension = new DimensionDictionary();
    }

    /**
     * Constructor.
     *
     * @param physical  Physical table dictionary
     * @param logical  Logical table dictionary
     * @param metric  Metric dictionary
     * @param dimension  Dimension dictionary
     */
    public ResourceDictionaries(
            final PhysicalTableDictionary physical,
            final LogicalTableDictionary logical,
            final MetricDictionary metric,
            final DimensionDictionary dimension
    ) {
        this.physical = physical;
        this.logical = logical;
        this.metric = metric;
        this.dimension = dimension;
    }

    public PhysicalTableDictionary getPhysicalDictionary() {
        return physical;
    }

    public LogicalTableDictionary getLogicalDictionary() {
        return logical;
    }

    public MetricDictionary getMetricDictionary() {
        return metric;
    }

    public DimensionDictionary getDimensionDictionary() {
        return dimension;
    }

    @Override
    public String toString() {
        return String.format(
                "Dimension dictionary: %s \n\n" +
                        "Metric dictionary: %s \n\n" +
                        "Logical Table dictionary: %s \n\n" +
                        "Physical Table dictionary: %s",
                dimension,
                metric.keySet(),
                logical,
                physical
        );
    }
}
