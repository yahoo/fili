// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;


import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;

import java.util.List;

/**
 * Physical Table Info Template.
 */
public interface PhysicalTableInfoTemplate {

    /**
     * Get physical table name.
     *
     * @return physical table name
     */
    String getName();

    /**
     * Get physical table description.
     *
     * @return physical table description
     */
    String getDescription();

    /**
     * Get physical table's metrics name.
     *
     * @return physical table's metrics
     */
    List<String> getMetrics();

    /**
     * Get physical table's dimensions.
     *
     * @return physical table's dimensions
     */
    List<String> getDimensions();

    /**
     * Get physical table's granularity.
     *
     * @return physical table's granularity
     */
    DefaultTimeGrain getGranularity();
}
