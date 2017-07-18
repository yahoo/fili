// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.config.dimension;

import com.yahoo.fili.webservice.data.dimension.impl.RegisteredLookupDimension;

import java.util.List;

/**
 * Defines the information needed to configure a RegisteredLookup Dimension.
 */
public interface RegisteredLookupDimensionConfig extends DimensionConfig {

    @Override
    default Class getType() {
        return RegisteredLookupDimension.class;
    }

    /**
     * Returns a list of lookups used to configure the Lookup dimension.
     *
     * @return List of lookups
     */
    List<String> getLookups();
}
