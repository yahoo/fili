package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_NOT_IN_TABLE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_UNDEFINED;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.PathSegment;

public class DefaultDimensionGenerator implements Generator<LinkedHashSet<Dimension>> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDimensionGenerator.class);

    // TODO what are prequisites for dimensions??? (logicalTable?)
    @Override
    public LinkedHashSet<Dimension> bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return generateDimensions(params.getDimensions(), resources.getDimensionDictionary());
    }

    // TODO document logical table CAN be null, indicates not yet built
    @Override
    public void validate(
            LinkedHashSet<Dimension> entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        if (builder.getLogicalTable() == null) {
            throw new UnsatisfiedApiRequestConstraintsException("Attempted to build data request Dimensions, " +
                    "but LogicalTable has not yet been generated. Dimensions depend on LogicalTable");
        }

        if (!builder.getLogicalTable().isPresent()) {
            throw new BadApiRequestException("No logical table specified. Data requests require exactly one logical" +
                    "table to be queried");
        }

        validateRequestDimensions(entity, builder.getLogicalTable().get());
    }

    /**
     * Extracts the list of dimension names from the url dimension path segments and generates a set of dimension
     * objects based on it.
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
