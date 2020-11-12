// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;

/**
 * Metadata for GenerateMetrics.
 *
 * Generated metrics don't require most metadata because they exist only during request processing.  They do require
 * a reference to their base configured metric to support validation.
 */
public class GeneratedMetricInfo extends LogicalMetricInfo {

    private final String baseMetricName;

    /**
     * Constructor.
     *
     * @param name  the column name for the resulting metric
     * @param baseMetricName  the name of the configured metric that is the basis for this generated metric.
     */
    public GeneratedMetricInfo(String name, String baseMetricName) {
        super(name);
        this.baseMetricName = baseMetricName;
    }

    /**
     * Getter.
     *
     * @return  The name of the configured metric in the MetricDictionary that this metric was built from.
     */
    public String getBaseMetricName() {
        return baseMetricName;
    }
}
