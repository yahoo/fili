// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.inject.Singleton;
import javax.validation.constraints.NotNull;

/**
 * Merges TemplateDruidQueries together.
 */
@Singleton
public class TemplateDruidQueryMerger {

    private static final Logger LOG = LoggerFactory.getLogger(TemplateDruidQuery.class);

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private final boolean requireMetricsInQueries;


    public TemplateDruidQueryMerger() {
        requireMetricsInQueries = SYSTEM_CONFIG.getBooleanProperty(
                AbstractBinderFactory.REQUIRE_METRICS_IN_QUERY_KEY,
                AbstractBinderFactory.REQUIRE_METRICS_IN_QUERY_DEFAULT
        );
    }

    /**
     * Merge all of the TemplateDruidQueries from all of the Metrics in an DataApiRequest together.
     *
     * @param request  DataApiRequest to get the metrics from
     *
     * @return The merged TemplateDruidQuery
     */
    public TemplateDruidQuery merge(DataApiRequest request) {
        Set<LogicalMetric> metrics = request.getLogicalMetrics();
        Set<TemplateDruidQuery> allQueries = new LinkedHashSet<>();

        // Gather all of the unique TemplateDruidQueries
        for (LogicalMetric metric : metrics) {
            TemplateDruidQuery query = metric.getTemplateDruidQuery();
            if (query != null) {
                allQueries.add(metric.getTemplateDruidQuery());
            }
        }

        if (requireMetricsInQueries && allQueries.isEmpty()) {
            LOG.debug("No template queries selected by API request.");
            throw new IllegalStateException("No template queries selected by API request.");
        }

        Iterator<TemplateDruidQuery> queries = allQueries.iterator();
        if (!queries.hasNext()) {
            return new TemplateDruidQuery(new ArrayList<>(0), new ArrayList<>(0));
        }

        TemplateDruidQuery merged = queries.next();
        while (queries.hasNext()) {
            TemplateDruidQuery query = queries.next();
            LOG.trace("Merging TDQs: Left: {} | right: {}", merged, query);
            merged = merged.merge(query);
        }

        LOG.trace("Merged template druid query: {}", merged);
        return merged;
    }
}
