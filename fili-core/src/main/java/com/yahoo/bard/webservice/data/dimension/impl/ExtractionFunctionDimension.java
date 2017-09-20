// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

import java.util.Optional;

/**
 * A dimension that supports extraction functions.
 */
public interface ExtractionFunctionDimension extends Dimension {

    /**
     * Build an extraction function model object.
     *
     * @return  Take the internal namespaces and construct a model object for the extraction functions.
     */
    Optional<ExtractionFunction> getExtractionFunction();
}
