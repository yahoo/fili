// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl;

import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.rfc_dimension.DomainSchema;

import java.util.function.BiFunction;

/**
 * Core implementation is based on JSON object serialization.  It's not super efficient.  Domain Schema will help when
 * taking fields from a wider storage (such as SQL)
 */
@FunctionalInterface
public interface DimensionRowFactory extends BiFunction<byte[], DomainSchema, DimensionRow> {
}
