// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.web.apirequest.binders.HavingGenerator;

import org.joda.time.DateTimeZone;

/**
 * Configuration used in the java servlet interface.
 */
public interface BardConfigResources {

    /**
     * The collection of resource dictionaries.
     *
     * @return A dictionary of resource dictionaries
     */
    ResourceDictionaries getResourceDictionaries();

    /**
     * A parser for string to granularities.
     *
     * @return A granularity parser
     */
    GranularityParser getGranularityParser();

    /**
     * A parser and builder for filters.
     *
     * @return A filter builder resource
     */
    DruidFilterBuilder getFilterBuilder();

    /**
     * Having Api Generator.
     *
     * @return  A Having Generator
     */
    HavingGenerator getHavingApiGenerator();

    /**
     * The configured default time zone for dates.
     *
     * @return A time zone
     */
    DateTimeZone getSystemTimeZone();

    /**
     * The dictionary of configured dimensions.
     *
     * @return A dictionary of dimensions
     */
    default DimensionDictionary getDimensionDictionary() {
        return getResourceDictionaries().getDimensionDictionary();
    }

    /**
     * The dictionary of configured metrics.
     *
     * @return A metric dictionary.
     */
    default MetricDictionary getMetricDictionary() {
        return getResourceDictionaries().getMetricDictionary();
    }

    /**
     * The dictionary of logical tables.
     *
     * @return A logical table dictionary
     */
    default LogicalTableDictionary getLogicalTableDictionary() {
        return getResourceDictionaries().getLogicalDictionary();
    }
}
