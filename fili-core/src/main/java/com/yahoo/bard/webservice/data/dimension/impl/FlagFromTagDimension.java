// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Build a Flag dimension with a simple true and false filter corresponding to a multivalued 'tag' dimension.
 */
public class FlagFromTagDimension extends RegisteredLookupDimension {

    private final FlagFromTagDimensionConfig dimensionConfig;

    private final Map<String, DimensionRow> rowMap;

    private final Dimension filteringDimension;
    private final String tagValue;
    private final String trueValue;
    private final String falseValue;


    /**
     * Create a flag dimension based on a tag dimension.
     *
     * A flag dimension has values true and false, and corresponds to another dimension that has or doesn't have
     * a given identifier in a multivalued list of identifiers.  The expected contract for the flag dimension
     * is that it will have one dimension that can be filtered using default serialization, substituting the
     * expression tag|key-in[true] with flag|key-eq[flagName] where flag is the targeted value.  The expectation
     * for grouping is that a comma delimited string dimension will be present for which a custom extraction function
     * will be used for grouping.
     *
     *
     * @param flagDimensionConfig  The dimension configuration for
     */
    public FlagFromTagDimension(FlagFromTagDimensionConfig config, DimensionDictionary dimensionDictionary) {
        super(config);
        this.dimensionConfig = config;
        this.filteringDimension = dimensionDictionary.findByApiName(config.getFilteringDimensionApiName());
        tagValue = dimensionConfig.getTagValue();
        trueValue = dimensionConfig.getTrueValue();
        falseValue = dimensionConfig.getFalseValue();
        this.rowMap = Collections.unmodifiableMap(config.getRowMap());
    }

    @Override
    public void addDimensionRow(DimensionRow dimensionRow) {
        throw new UnsupportedOperationException("Dimension values for Tag Dimensions are immutable");
    }

    @Override
    public void addAllDimensionRows(Set<DimensionRow> dimensionRows) {
        throw new UnsupportedOperationException("Dimension values for Tag Dimensions are immutable");
    }

    @Override
    public DimensionRow findDimensionRowByKeyValue(String value) {
        return rowMap.getOrDefault(value, null);
    }

    public Dimension getFilteringDimension() {
        return filteringDimension;
    }

    public String getTagValue() {
        return tagValue;
    }

    public String getTrueValue() {
        return trueValue;
    }

    public String getFalseValue() {
        return falseValue;
    }
}
