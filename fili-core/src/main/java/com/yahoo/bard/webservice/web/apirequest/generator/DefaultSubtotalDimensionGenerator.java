// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Default generator implementation for async after.
 */
public class DefaultSubtotalDimensionGenerator implements Generator<List<List<String>>> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSubtotalDimensionGenerator.class);

    @Override
    public List<List<String>> bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return generateSubtotalDimensions(params.getSubtotals().orElse(null));
    }

    @Override
    public void validate(
            List<List<String>> entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no validation
    }

    /**
     * Parses the substotal String representation into a list of subsets of dimensions to be used for building
     * subtotals.
     *
     * @param subtotals  subtotals should be null, or a string of the form [['d1','d2'],['d1']]
     *
     * @return A long describing how long the user is willing to wait
     *
     * @throws BadApiRequestException if the string is not null but can not be parsed.
     */
    public static List<List<String>> generateSubtotalDimensions(String subtotals) throws BadApiRequestException {
        String localSubtotals = subtotals;
        if (localSubtotals == null) {
            return Collections.emptyList();
        }
        // TODO parse subtotal string
        return Collections.emptyList();
    }
}
