// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory;
import com.yahoo.bard.webservice.web.apirequest.generator.LegacyGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.having.HavingGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.ApiRequestLogicalMetricBinder;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.DefaultLogicalMetricGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.orderBy.DefaultOrderByGenerator;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

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
     * The date time formatter.
     *
     * @return the formatter
     *
     */
    default DateTimeFormatter getDateTimeFormatter() {
        return DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER;
    }

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

    /**
     * The default transforms to apply to ApiMetrics after parsing before binding.
     *
     * @return a function to conform apiMetrics according to business requirements.
     */
    default ApiRequestLogicalMetricBinder getMetricBinder() {
        return new DefaultLogicalMetricGenerator();
    }

    /**
     * The factory to parse and bind order by expressions.
     *
     * @return a function to conform apiMetrics according to business requirements.
     */
    default LegacyGenerator<List<OrderByColumn>> getOrderByGenerator() {
        return new DefaultOrderByGenerator();
    }
}
