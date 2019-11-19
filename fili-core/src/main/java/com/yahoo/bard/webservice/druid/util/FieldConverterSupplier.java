// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.util;

import com.yahoo.bard.webservice.web.MetricsFilterSetBuilder;

/**
 * Supplies global access points to utility operations that work on sketch-like objects.
 */
public final class FieldConverterSupplier {

    private static FieldConverters sketchConverter;
    private static MetricsFilterSetBuilder metricsFilterSetBuilder;


    /**
     * Dummy private constructor to prevent instantiation of utility class.
     */
    private FieldConverterSupplier() {

    }

    public static FieldConverters getSketchConverter() {
        return sketchConverter;
    }


    public static void setSketchConverter(FieldConverters sketchConverter) {
        FieldConverterSupplier.sketchConverter = sketchConverter;
    }


    public static MetricsFilterSetBuilder getMetricsFilterSetBuilder() {
        return metricsFilterSetBuilder;
    }


    public static void setMetricsFilterSetBuilder(MetricsFilterSetBuilder metricsFilterSetBuilder) {
        FieldConverterSupplier.metricsFilterSetBuilder = metricsFilterSetBuilder;
    }
}
