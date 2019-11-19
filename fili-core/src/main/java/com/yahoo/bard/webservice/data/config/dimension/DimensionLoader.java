// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;

/**
 * Defines the core interactions for loading dimensions into a dimension dictionary.
 */
@FunctionalInterface
public interface DimensionLoader {

    /**
     * Load dimensions and populate the dimension dictionary.
     *
     * @param dimensions  The dictionary that will be loaded with dimensions
     */
    void loadDimensionDictionary(DimensionDictionary dimensions);

}
