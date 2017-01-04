/*
 * // Copyright 2017 Yahoo Inc.
 * // Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
 *
 */
package com.yahoo.bard.webservice.rfc.table;

import com.yahoo.bard.webservice.druid.model.query.Granularity;

/**
 * Interface to support objects which have granularities
 */
public interface HasGranularity {
    public Granularity getGranularity();
}
