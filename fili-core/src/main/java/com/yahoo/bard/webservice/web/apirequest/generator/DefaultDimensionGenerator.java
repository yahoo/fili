// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_UNDEFINED;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.DIMENSIONS;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.LOGICAL_TABLE;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.PathSegment;

/**
 * Default Generator implementation for generating grouping {@link Dimension}s. Dimensions are dependent on the
 * {@link LogicalTable} they were queried on. Ensure the queried logical table has been bound before using this
 * generator to bind dimensions.
 */
public class DefaultDimensionGenerator implements Generator<LinkedHashSet<Dimension>> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDimensionGenerator.class);

    @Override
    public LinkedHashSet<Dimension> bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return generateDimensions(params.getDimensions(), resources.getDimensionDictionary());
    }

    /**
     * Validates that the bound set of dimensions is valid against the queried logical table.
     *
     * Throws {@link UnsatisfiedApiRequestConstraintsException} if the queried logical table has not yet been bound.
     * Throws {@link BadApiRequestException} if no logical table was specified in the query.
     *
     * @param entity  The resource constructed by the {@code bind}} method
     * @param builder  The builder object representing the in progress DataApiRequest
     * @param params  The request parameters sent by the client
     * @param resources  Resources used to build the request
     */
    @Override
    public void validate(
            LinkedHashSet<Dimension> entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        if (!builder.isLogicalTableInitialized()) {
            throw new UnsatisfiedApiRequestConstraintsException(
                    DIMENSIONS.getResourceName(),
                    Collections.singleton(LOGICAL_TABLE.getResourceName())
            );
        }

        if (!builder.getLogicalTableIfInitialized().isPresent()) {
            throw new BadApiRequestException("No logical table specified. Data requests require exactly one logical" +
                    "table to be queried");
        }

        validateRequestDimensions(entity, builder.getLogicalTableIfInitialized().get());
    }

    /**
     * Extracts the list of dimension names from the url dimension path segments and generates a set of dimension
     * objects based on it.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param apiDimensions  Dimension path segments from the URL.
     * @param dimensionDictionary  Dimension dictionary contains the map of valid dimension names and dimension objects.
     *
     * @return Set of dimension objects.
     * @throws BadApiRequestException if an invalid dimension is requested.
     */
    public static LinkedHashSet<Dimension> generateDimensions(
            List<PathSegment> apiDimensions,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingDimensions")) {
            // Dimensions are optional hence check if dimensions are requested.
            if (apiDimensions == null || apiDimensions.isEmpty()) {
                return new LinkedHashSet<>();
            }

            // set of dimension names (strings)
            List<String> dimApiNames = apiDimensions.stream()
                    .map(PathSegment::getPath)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());

            // set of dimension objects
            LinkedHashSet<Dimension> generated = new LinkedHashSet<>();
            List<String> invalidDimensions = new ArrayList<>();
            for (String dimApiName : dimApiNames) {
                Dimension dimension = dimensionDictionary.findByApiName(dimApiName);

                // If dimension dictionary returns a null, it means the requested dimension is not found.
                if (dimension == null) {
                    invalidDimensions.add(dimApiName);
                } else {
                    generated.add(dimension);
                }
            }

            if (!invalidDimensions.isEmpty()) {
                LOG.debug(DIMENSIONS_UNDEFINED.logFormat(invalidDimensions.toString()));
                throw new BadApiRequestException(DIMENSIONS_UNDEFINED.format(invalidDimensions.toString()));
            }

            LOG.trace("Generated set of dimension: {}", generated);
            return generated;
        }
    }

    /**
     * Ensure all request dimensions are part of the logical table.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param requestDimensions  The dimensions being requested
     * @param table  The logical table being checked
     *
     * @throws BadApiRequestException if any of the dimensions do not match the logical table
     */
    public static void validateRequestDimensions(Set<Dimension> requestDimensions, LogicalTable table)
            throws BadApiRequestException {
        // Requested dimensions must lie in the logical table
        requestDimensions = new HashSet<>(requestDimensions);
        requestDimensions.removeAll(table.getDimensions());

        if (!requestDimensions.isEmpty()) {
            List<String> dimensionNames = requestDimensions.stream()
                    .map(Dimension::getApiName)
                    .collect(Collectors.toList());
            LOG.debug(DIMENSIONS_NOT_IN_TABLE.logFormat(dimensionNames, table.getName()));
            throw new BadApiRequestException(DIMENSIONS_NOT_IN_TABLE.format(dimensionNames, table.getName()));
        }
    }
}
