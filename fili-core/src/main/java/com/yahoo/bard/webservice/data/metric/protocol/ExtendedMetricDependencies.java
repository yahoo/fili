// Copyright 2022 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Contract for a metric that has extended depdendencies such as dependent metrics and extended intervals consumed.
 */
public interface ExtendedMetricDependencies {

    /**
     * Gets the dependent metrics of this metric.
     *
     * @return the dependent metrics
     */
    default List<LogicalMetric> getDependentMetrics() {
        return Collections.emptyList();
    }

    /**
     * Gets all dependencies of this metric and all of its dependent metrics.
     * For example, for the following dependency graph with root node (a):
     *
     *     b
     *   /
     * a     d
     *  \  /
     *   c
     *    \
     *     e
     *
     * a.getTransitiveDependencies() should return b, c, d, e.
     *
     * TODO should a consistent ordering be required?
     *
     * @return A stream that produces all transitive dependencies of the current metric
     */
    default Stream<LogicalMetric> getTransitiveDependencies() {

        return Stream.concat(
                getDependentMetrics().stream(),
                getDependentMetrics()
                        .stream()
                        .flatMap(d -> (d instanceof ExtendedMetricDependencies
                                        ? ((ExtendedMetricDependencies) d).getTransitiveDependencies()
                                        : Stream.empty()
                                )
                        )
        );
    }


    /**
     * Calculates the interval in the underlying datasource that will be referenced by this metric.
     *
     * @param requestInterval  The interval
     * @param requestGrain  The stretched period
     *
     * @return the extended interval
     */
    default SimplifiedIntervalList getDependentInterval(
            SimplifiedIntervalList requestInterval,
            Granularity requestGrain
    ) {
        return requestInterval;
    }
}
