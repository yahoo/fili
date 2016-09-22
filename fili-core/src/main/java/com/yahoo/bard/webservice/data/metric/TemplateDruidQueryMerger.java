// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.web.DataApiRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.inject.Singleton;

/**
 * Merges TemplateDruidQueries together.
 */
@Singleton
public class TemplateDruidQueryMerger {

    private static final Logger LOG = LoggerFactory.getLogger(TemplateDruidQuery.class);

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

        if (allQueries.isEmpty()) {
            LOG.debug("No template queries selected by API request.");
            throw new IllegalStateException("No template queries selected by API request.");
        }

        // TODO: Simplify when merge can handle nulls
        TemplateDruidQuery merged = null;
        for (TemplateDruidQuery query : allQueries) {
            if (merged == null) {
                merged = query;
            } else {
                LOG.trace("Merging TDQs: Left: {} | right: {}", merged, query);
                merged = merged.merge(query);
            }
        }

        LOG.trace("Merged template druid query: {}", merged);
        return merged;
    }
}
