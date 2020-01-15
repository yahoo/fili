// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.model.MetricField;

import java.util.Collection;

/**
 * A LogicalMetric is a set of its TemplateQueries, Mapper, and its name.
 */
public interface LogicalMetric {
    String DEFAULT_CATEGORY = "General";

    /**
     * The name used in api and the dictionary for this metric.
     *
     * @return A name
     */
    String getName();

    /**
     *  A human-friendly name for this metric.
     *
     * @return A long name
     */
    String getLongName();


    /**
     * Description information for this metric and it's definition.
     *
     * @return a description
     */
    String getDescription();

    /**
     * The post processing stage for this metric.
     *
     * @return a ResultSetMapper to be run in the response processing.
     */
    ResultSetMapper getCalculation();

    /**
     * The physical query model for this metric.
     *
     * @return A template druid query.
     */
    TemplateDruidQuery getTemplateDruidQuery();

    /**
     * The field in the query model representing the value of this metric.
     *
     * @return  An aggregation or post aggregation appearing in the template druid query.
     */
    MetricField getMetricField();

    /**
     * The category for grouping this metric.
     *
     * @return A string representing a logical category.
     */
    String getCategory();

    /**
     * The type of the value of this metric.
     *
     * @return A string defining the type of the metric.
     */
    String getType();

    /**
     * Does this metric support being rebuilt.
     *
     * @return true if this metric supports a regeneration contract.
     */
    boolean supportsRegeneration();

    /**
     * Does this metric accept these parameters.
     *
     * @param parameters  The names of parameters that this metric will accept.
     *
     * @return true if some of these parameters are accepted by the underlying metric
     */
    default boolean acceptsParameters(Collection<String> parameters) {
        return false;
    }
}
