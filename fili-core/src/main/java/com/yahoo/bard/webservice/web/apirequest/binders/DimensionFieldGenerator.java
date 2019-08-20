// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.Incubating;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import javax.ws.rs.core.PathSegment;

/**
 * Generates the a mapping of dimension to dimension field based on the queried dimension and relevant fields.
 */
@Incubating
public interface DimensionFieldGenerator {
    // Binders and Validators complete

    /**
     * Extracts the list of dimensions from the url dimension path segments and "show" matrix params and generates a map
     * of dimension to dimension fields which needs to be annotated on the response.
     * <p>
     * If no "show" matrix param has been set, it returns the default dimension fields configured for the dimension.
     *
     * @param apiDimensionPathSegments  Path segments for the dimensions
     * @param dimensionDictionary  Dimension dictionary to look the dimensions up in
     *
     * @return A map of dimension to requested dimension fields
     *
     * @incubating PathSegment should be REMOVED from this interface as soon as possible. PathSegment ties this class
     * to the web request input format. Generator classes should be ambivalent about the request ingestion
     * implementation.
     */
    @Incubating
    LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> generateDimensionFields(
            List<PathSegment> apiDimensionPathSegments,
            DimensionDictionary dimensionDictionary
    );

    /**
     * Validated dimension field objects.
     *
     * @param apiDimensionPathSegments  Path segments for the dimensions
     * @param perDimensionFields  The bound dimension fields for this query
     * @param dimensions  The bound dimensions for this query
     * @param logicalTable  The logical table for this query
     * @param dimensionDictionary  Dimension dictionary to look the dimensions up in
     *
     * @incubating PathSegment should be REMOVED from this interface as soon as possible. PathSegment ties this class
     * to the web request input format. Generator classes should be ambivalent about the request ingestion
     * implementation.
     */
    @Incubating
    void validateDimensionFields(
            List<PathSegment> apiDimensionPathSegments,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<Dimension> dimensions,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    );

    /**
     * Default implementation.
     */
    DimensionFieldGenerator DEFAULT_DIMENSION_FIELD_GENERATOR = new DefaultDimensionFieldGenerator();
}
