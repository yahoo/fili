// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints.views;

/**
 * Standard formatters for metadata output.
 */
public class DefaultMetadataViewFormatters {

    public static TableMetadataFormatter tableMetadataFormatter = TableMetadataFormatter.INSTANCE;
    public static TableFullViewFormatter rollupMetadataFormatter = TableFullViewFormatter.INSTANCE;
    public static MetricMetadataFormatter metricMetadataFormatter = MetricMetadataFormatter.INSTANCE;
    public static DimensionMetadataFormatter dimensionMetadataFormatter = DimensionMetadataFormatter.INSTANCE;

    /**
     * Make a unconstructable utility class.
     */
    private DefaultMetadataViewFormatters() {
    }
}
