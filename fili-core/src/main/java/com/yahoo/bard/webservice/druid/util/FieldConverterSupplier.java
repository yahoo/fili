// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.util;

import com.yahoo.bard.webservice.web.MetricsFilterSetBuilder;

/**
 * Supplies global access points to utility operations that work on sketch-like objects.
 */
public final class FieldConverterSupplier {
    public static FieldConverters sketchConverter;
    public static MetricsFilterSetBuilder metricsFilterSetBuilder;
}
