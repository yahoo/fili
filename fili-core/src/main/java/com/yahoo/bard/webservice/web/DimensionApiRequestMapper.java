// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.FILTER_DIMENSION_MISMATCH;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Default RequestMapper implementation for DimensionApiRequests.
 */
public class DimensionApiRequestMapper extends RequestMapper<DimensionsApiRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionApiRequestMapper.class);

    /**
     * Constructor.
     *
     * @param resourceDictionaries  Dictionaries to use when mapping
     */
    public DimensionApiRequestMapper(@NotNull ResourceDictionaries resourceDictionaries) {
        super(resourceDictionaries);
    }

    /**
     * Validate that dimensionApiRequest should not contain filter whose dimensions do not match requested dimension.
     *
     * @param request  the apiRequest to rewrite
     * @param context  the ContainerRequestContext
     *
     * @return the original dimensionApiRequest if validated
     *
     * @throws RequestValidationException when the filter contains dimensions that do not match requested dimension
     */
    @Override
    public DimensionsApiRequest apply(DimensionsApiRequest request, ContainerRequestContext context)
            throws RequestValidationException {
        Set<Dimension> requestedDimensions = request.getDimensions();

        for (ApiFilter filter : request.getFilters()) {
            if (!requestedDimensions.contains(filter.getDimension())) {
                Dimension filterDimension = filter.getDimension();
                String msg;
                if (requestedDimensions.size() == 1) {
                    msg = FILTER_DIMENSION_MISMATCH.logFormat(
                            filterDimension.getApiName(),
                            requestedDimensions.iterator().next()
                    );
                } else {
                    msg = FILTER_DIMENSION_MISMATCH.logFormat(filterDimension.getApiName(), "set");
                }
                LOG.error(msg);
                throw new RequestValidationException(BAD_REQUEST, msg, msg);
            }
        }

        return request;
    }
}
