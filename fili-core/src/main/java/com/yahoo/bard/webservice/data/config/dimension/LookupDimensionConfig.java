// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension;

import java.util.List;

/**
 * Defines the information needed to configure a Lookup Dimension.
 */
public interface LookupDimensionConfig extends DimensionConfig {

    @Override
    default Class getType() {
        return LookupDimension.class;
    }

    /**
     * Returns a list of namespaces used to configure the Lookup dimension.
     *
     * @return List of namespaces
     */
    List<String> getNamespaces();
}
