// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.util.ScopeMap;

import javax.inject.Singleton;

/**
 * Map of MetricName to LogicalMetric.  Metric Dictionary supports metric configuration as well as providing metric
 * definitions at query resolution time.
 * <PRE>
 * At configuration time metrics are loaded into the metric dictionary, global by default:
 *      globalMetricDictionary.add(LogicalMetric)
 * or for metrics within a single level of scope named "scope", either:
 *      globalMetricDictionary.putScope("scope", logicalMetric.getName(), logicalMetric)
 * or
 *      MetricDictionary scopedDictionary = globalMetricDictionary.getScope("scope")
 *      scopedDictionary.add(logicalMetric)
 *
 * With more than one scope of nesting use:
 *      MetricDictionary scopedDictionary = globalMetricDictionary.getScope("scope1").getScope("scope2")
 * or
 *      scopedDictionary = metricDictionary.getScope("scopeOuter", "scopeMiddle", "scopeInner")
 *
 * At queryTime: with scope "scope":
 *   metricDictionary =  metricDictionary.get("scope")
 *   LogicalMetric lm = metricDictionary.get("key")
 *
 * will return values in both "scope" and in global scope.
 * </PRE>
 */
@Singleton
public class MetricDictionary extends ScopeMap<String, String, LogicalMetric, MetricDictionary> {

    /**
     * Create a metric dictionary with global scope.
     */
    public MetricDictionary() {
        super();
    }

    /**
     * Create a metric dictionary whose parent scope is scope.
     *
     * @param parentScope  The parent scope to this scope
     */
    private MetricDictionary(ScopeMap<String, String, LogicalMetric, MetricDictionary> parentScope) {
        super(parentScope);
    }

    /**
     * Add a logical metric to the dictionary.
     *
     * @param logicalMetric  Logical metric to add
     *
     * @return True if the Logical Metric did not exist in the dictionary before, false if it did
     */
    public boolean add(LogicalMetric logicalMetric) {
        return this.put(logicalMetric.getName(), logicalMetric) == null;
    }

    @Override
    protected MetricDictionary factory(MetricDictionary scope) {
        return new MetricDictionary(scope);
    }
}
