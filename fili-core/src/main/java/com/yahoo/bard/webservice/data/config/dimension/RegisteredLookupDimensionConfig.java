// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

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
     * Returns a list of extraction functions for the lookup dimension values.
     *
     * @return the list of extraction functions for the lookup dimension values
     */
    List<ExtractionFunction> getRegisteredLookupExtractionFns();
}
