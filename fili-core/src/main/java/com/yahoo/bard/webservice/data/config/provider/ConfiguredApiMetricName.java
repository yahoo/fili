// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

/**
 * An ApiMetricName built from configuration.
 *
 * Perhaps not totally complete:
 * - Should there be a distinction between apiName and asName?
 * - Is the isValidFor implementation correct?
 */
public class ConfiguredApiMetricName implements ApiMetricName {
    protected String name;
    protected LogicalTableConfiguration logicalTable;

    /**
     * Construct a new configured API metric name.
     *
     * @param name the name
     * @param logicalTable the logical table the metric belongs to
     */
    public ConfiguredApiMetricName(String name, LogicalTableConfiguration logicalTable) {
        this.name = name;
        this.logicalTable = logicalTable;
    }

    @Override
    public boolean isValidFor(final TimeGrain grain) {
        return logicalTable.getTimeGrains()
                .stream()
                .anyMatch(grain::satisfiedBy);
    }

    @Override
    public String getApiName() {
        return name;
    }

    @Override
    public String asName() {
        return name;
    }
}
