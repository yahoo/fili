// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.function.Function;

/**
 * RequestedIntervalsFunction is an implementation of Function that takes a DruidAggregationQuery and returns a
 * SimplifiedIntervalList. The purpose of this empty interface was to avoid using the long signature of the Function
 * time and again.
 */
public interface RequestedIntervalsFunction extends Function<DruidAggregationQuery<?>, SimplifiedIntervalList> {

}
