// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.orderby;

/**
 * The type of the column name in the order by Column.
 */
public enum OrderByColumnType {

    UNKNOWN,
    TIME,
    METRIC,
    // Unsupported in current parsing
    DIMENSION
}
