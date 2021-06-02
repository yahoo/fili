package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSIONS_NOT_IN_TABLE;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.VirtualDimension;
import com.yahoo.bard.webservice.data.dimension.impl.SimpleVirtualDimension;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class VirtualDimensionDimensionGenerator extends DefaultDimensionGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualDimensionDimensionGenerator.class);

    public static final VirtualDimensionDimensionGenerator INSTANCE = new VirtualDimensionDimensionGenerator();

    /**
     * Lookup dimension from dimension dictionary or other source.
     *
     * @param dimensionDictionary  The dimension dictionary
     * @param dimApiName  The api name for the dimension to be found
     *
     * @return A resolved dimension or null if the name cannot be resolved.
     */
    protected Dimension resolveDimension(
            final DimensionDictionary dimensionDictionary,
            final String dimApiName
    ) {
        Dimension dimension = dimensionDictionary.findByApiName(dimApiName);
        if (dimension == null && dimApiName.startsWith("__")) {
            return new SimpleVirtualDimension(dimApiName);
        }
        return dimension;
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
    public void validateRequestDimensions(Set<Dimension> requestDimensions, LogicalTable table)
            throws BadApiRequestException {
        // Requested dimensions must lie in the logical table
        List<Dimension> invalids = requestDimensions.stream()
                .filter(dim -> table.getDimensions().contains(dim))
                .filter(dim -> dim instanceof VirtualDimension)
                .collect(Collectors.toList());

        if (!invalids.isEmpty()) {
            List<String> dimensionNames = invalids.stream()
                    .map(Dimension::getApiName)
                    .collect(Collectors.toList());
            LOG.debug(DIMENSIONS_NOT_IN_TABLE.logFormat(dimensionNames, table.getName()));
            throw new BadApiRequestException(DIMENSIONS_NOT_IN_TABLE.format(dimensionNames, table.getName()));
        }
    }
}
