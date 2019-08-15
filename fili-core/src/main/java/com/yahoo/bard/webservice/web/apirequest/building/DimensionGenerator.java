// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.building;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_UNDEFINED;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.BadApiRequestException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.PathSegment;


// TODO to test this, use AggregatabilityValidationSpec. Do this once DataApiRequestImpl is switched over.
/**
 * Generator that creates a set of dimensions based on path segments.
 */
public interface DimensionGenerator {

    /**
     * Extracts the list of dimension names from the url dimension path segments and generates a set of dimension
     * objects based on it.
     *
     * @param apiDimensions  Dimension path segments from the URL.
     * @param dimensionDictionary  Dimension dictionary contains the map of valid dimension names and dimension objects.
     *
     * @return Set of dimension objects.
     */
    LinkedHashSet<Dimension> generateDimensions(
            List<PathSegment> apiDimensions,
            DimensionDictionary dimensionDictionary
    );

    /**
     * Ensure all request dimensions are part of the logical table.
     *
     * @param requestDimensions  The dimensions being requested
     * @param table  The logical table being checked
     */
    void validateRequestDimensions(Set<Dimension> requestDimensions, LogicalTable table);

    /**
     * Default implementation of this interface. Simply loops over the path segments, checks if the apiName in the
     * segment matches an apiName of a dimension in the DimensionDictionary, and if so adds it to the list. If any
     * of the path segments are not valid dimensions an error is thrown.
     */
    DimensionGenerator DEFAULT_DIMENSION_GENERATOR =
        new DimensionGenerator() {
            @Override
            public LinkedHashSet<Dimension> generateDimensions(
                    List<PathSegment> apiDimensions,
                    DimensionDictionary dimensionDictionary
            ) {
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
                        throw new BadApiRequestException(DIMENSIONS_UNDEFINED.format(invalidDimensions.toString()));
                    }
                    return generated;
                }
            }

            @Override
            public void validateRequestDimensions(
                    Set<Dimension> requestDimensions,
                    LogicalTable table
            ) {
                // Requested dimensions must lie in the logical table
                requestDimensions = new HashSet<>(requestDimensions);
                requestDimensions.removeAll(table.getDimensions());

                if (!requestDimensions.isEmpty()) {
                    List<String> dimensionNames = requestDimensions.stream()
                            .map(Dimension::getApiName)
                            .collect(Collectors.toList());
                    throw new BadApiRequestException(DIMENSIONS_NOT_IN_TABLE.format(dimensionNames, table.getName()));
                }
            }
        };
}
