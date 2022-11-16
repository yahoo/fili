// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricType;

/**
 * Metadata for GenerateMetrics.
 *
 * Generated metrics don't require most metadata because they exist only during request processing.  They do require
 * a reference to their base configured metric to support validation.
 */
public class GeneratedMetricInfo extends LogicalMetricInfo {

    private final String baseMetricName;

    private final MetricType modifiedType;

    /**
     * Constructor.
     *
     * @param name  the column name for the resulting metric
     * @param baseMetricName  the name of the configured metric that is the basis for this generated metric.
     * @param metricType a metric type that overrides the type of the baseMetricName
     */
    public GeneratedMetricInfo(String name, String baseMetricName, MetricType metricType) {
        super(name);
        this.baseMetricName = baseMetricName;
        this.modifiedType = metricType;
    }

    /**
     * Constructor.
     *
     * @param name  the column name for the resulting metric
     * @param baseMetricName  the name of the configured metric that is the basis for this generated metric.
     */
    public GeneratedMetricInfo(String name, String baseMetricName) {
        this(name, baseMetricName, null);
    }

    /**
     * Getter.
     *
     * @return  The name of the configured metric in the MetricDictionary that this metric was built from.
     */
    public String getBaseMetricName() {
        return baseMetricName;
    }

    /**
     * Returns the type of the metric.
     *
     * @return the type of the metric
     */
    public MetricType getType() {
        return modifiedType == null ? super.getType() : modifiedType;
    }

    /**
     * Copy this metric info with a modified type.
     *
     * @param metricType  the metric type to replace with.
     *
     * @return A logical metric info with a modified type.
     */
    public LogicalMetricInfo withType(MetricType metricType) {
        return new GeneratedMetricInfo(getName(), baseMetricName, metricType);
    }
}
