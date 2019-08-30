// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.MetricField;

/**
 * Logical Metrics model calculations performed in a request.
 */
public interface LogicalMetric {

    /**
     * The name of the metric (generally apiName and metric dictionary name).
     *
     * @return A name
     */
    String getName();

    /**
     * The human friendly name of the metric (generally apiName and metric dictionary name).
     *
     * Long name is sometimes used for output column annotation.
     *
     * @return A name
     */
    String getLongName();

    /**
     * The description of the metric.
     *
     * @return A description
     */
    String getDescription();


    /**
     * The category of the metric.
     *
     * @return A grouping identity, often used in UIs
     */
    String getCategory();


    /**
     * The type of the metric.
     *
     * @return A named type, often 'number'.
     */
    String getType();

    /**
     * An object encapsulating all the descriptor metadata.
     *
     * @return A value object describing this metric's metadata.
     */
    LogicalMetricInfo getLogicalMetricInfo();

    /**
     * The validaity for this metric on a given granularity.
     *
     * @param granularity  The granularity being tested
     *
     * @return true if the metric is presumed valid for this granularity.
     */
    default boolean isValidFor(Granularity granularity) {
        return true;
    }

    /**
     * Post processing mapper for this metric.
     *
     * @return A mapping function.
     */
    ResultSetMapper getCalculation();

    /**
     * The physical query performed to create this metric.
     *
     * @return A partial physical query.
     */
    TemplateDruidQuery getTemplateDruidQuery();

    /**
     * The column in the fact query set corresponding to this metric.
     *
     * @return  A schema column in the template query.
     */
    MetricField getMetricField();
}
