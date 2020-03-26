// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric;

import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric;

import java.util.List;

public interface ApiMetricParser {
    List<ApiMetric> apply(
            String metricQuery
    ) throws BadApiRequestException;
}
