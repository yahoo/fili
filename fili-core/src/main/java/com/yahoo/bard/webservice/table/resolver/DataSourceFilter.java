// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import java.util.function.Function;

/**
 * A function which filters based on a data source constraint.
 */
@FunctionalInterface
public interface DataSourceFilter extends Function<DataSourceConstraint, Boolean> {
}
