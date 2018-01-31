// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.web.ApiHaving;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Generates having maps from having strings.
 */
public interface HavingGenerator extends BiFunction<String, Set<LogicalMetric>, Map<LogicalMetric, Set<ApiHaving>>> {

}
