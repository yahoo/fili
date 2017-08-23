// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

import java.util.Optional;

/**
 * A dimension that supports extraction functions.
 */
public abstract class ExtractionFunctionDimension extends KeyValueStoreDimension {

    /**
     * Constructor.
     *
     * @param lookupDimensionConfig  The configuration for the underlying dimension.
     */
    public ExtractionFunctionDimension(DimensionConfig lookupDimensionConfig) {
        super(lookupDimensionConfig);
    }

    /**
     * Build an extraction function model object.
     *
     * @return  Take the internal namespaces and construct a model object for the extraction functions.
     */
    public abstract Optional<ExtractionFunction> getExtractionFunction();
}
