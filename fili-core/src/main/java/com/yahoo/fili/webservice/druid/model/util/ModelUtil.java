// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.util;

import com.yahoo.fili.webservice.data.dimension.Dimension;
import com.yahoo.fili.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.fili.webservice.data.dimension.impl.LookupDimension;
import com.yahoo.fili.webservice.data.dimension.impl.RegisteredLookupDimension;
import com.yahoo.fili.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction;
import com.yahoo.fili.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.fili.webservice.druid.model.dimension.extractionfunction.LookupExtractionFunction;
import com.yahoo.fili.webservice.druid.model.dimension.extractionfunction.NamespaceLookup;
import com.yahoo.fili.webservice.druid.model.dimension.extractionfunction.RegisteredLookupExtractionFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

            List<ExtractionFunction> extractionFunctions = lookupDimension.getNamespaces().stream()
                    .map(
                            namespace -> new LookupExtractionFunction(
                                    new NamespaceLookup(namespace),
                                    false,
                                    "Unknown " + namespace,
                                    false,
                                    true
                            )
                    ).collect(Collectors.toList());

            return Optional.ofNullable(
                    extractionFunctions.size() > 1 ?
                            new CascadeExtractionFunction(extractionFunctions) :
                            extractionFunctions.size() == 1 ? extractionFunctions.get(0) : null
            );
        } else if (sourceClass.equals(RegisteredLookupDimension.class)) {
            RegisteredLookupDimension registeredLookupDimension = (RegisteredLookupDimension) dimension;

            List<ExtractionFunction> extractionFunctions = registeredLookupDimension.getLookups().stream()
                    .map(
                            lookup -> new RegisteredLookupExtractionFunction(
                                    lookup,
                                    false,
                                    "Unknown " + lookup,
                                    false,
                                    true
                            )
                    ).collect(Collectors.toList());

            return Optional.ofNullable(
                    extractionFunctions.size() > 1 ?
                            new CascadeExtractionFunction(extractionFunctions) :
                            extractionFunctions.size() == 1 ? extractionFunctions.get(0) : null
            );
        }

        LOG.debug("Could not resolve ExtractionFunction from the provided Dimension: {}", sourceClass.toString());
        return Optional.empty();
    }
}
