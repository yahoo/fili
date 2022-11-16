// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.metrics;

import java.util.function.Function;

/**
 * Interface to assist with applying business rules to mapped apimetrics.
 */
public interface ApiMetricAnnotater extends Function<ApiMetric, ApiMetric> {
    ApiMetricAnnotater NO_OP_ANNOTATER = a -> a;
}
