// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.util;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension;
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Utility functions related to druid models.
 */
public class ModelUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ModelUtil.class);

    /**
     * Resolves the given Dimension to a corresponding Dimension ExtractionFunction.
     *
     * @param dimension  the Dimension to be resolved to ExtractionFunction.
     *
     * @return  an optional ExtractionFunction determined by the type of Dimension provided
     */
    public static Optional<ExtractionFunction> getExtractionFunction(Dimension dimension) {
        Class<?> sourceClass = dimension.getClass();

        if (sourceClass.equals(KeyValueStoreDimension.class)) {
            return Optional.empty();
        } else if (sourceClass.equals(LookupDimension.class)) {
            LookupDimension lookupDimension = (LookupDimension) dimension;
            return lookupDimension.getExtractionFunction();
        } else if (sourceClass.equals(RegisteredLookupDimension.class)) {
            RegisteredLookupDimension registeredLookupDimension = (RegisteredLookupDimension) dimension;
            return registeredLookupDimension.getExtractionFunction();
        }

        LOG.debug("Could not resolve ExtractionFunction from the provided Dimension: {}", sourceClass.toString());
        return Optional.empty();
    }
}
