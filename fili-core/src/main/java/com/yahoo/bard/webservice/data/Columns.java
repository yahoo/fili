// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

/**
 * A generic, high level enumeration to distinguish between dimensions and metrics. Mainly to be used in collections
 * (i.e. EnumMap). It is independent of table types (e.g physical or logical tables)
 */
public enum Columns {
    DIMENSIONS,
    METRICS
}
