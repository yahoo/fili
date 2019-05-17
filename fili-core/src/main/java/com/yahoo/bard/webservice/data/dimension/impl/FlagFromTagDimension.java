// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField;
import com.yahoo.bard.webservice.data.config.dimension.DefaultRegisteredLookupDimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.TagExtractionFunctionFactory;

import org.joda.time.DateTime;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Build a Flag dimension with a simple true and false filter corresponding to a multivalued 'tag' dimension.
 */
public class FlagFromTagDimension implements Dimension {

    private final FlagFromTagDimensionConfig dimensionConfig;

    private final DimensionRow trueRow;
    private final DimensionRow falseRow;

    private static final String NULL_VALUE = "";
    private static final String UNUSED_PHYSICAL_NAME = "unused_physical_name";

    private final Dimension groupingDimension;
    private final Dimension filteringDimension;

    protected final Map<String, DimensionRow> rowMap;
    protected SearchProvider searchProvider;


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
     * @param dimensionDictionary The dictionary containing the dependant dimensions
     */
    public FlagFromTagDimension(
            FlagFromTagDimensionConfig flagDimensionConfig,
            DimensionDictionary dimensionDictionary
    ) {
        this.dimensionConfig = flagDimensionConfig;
        this.filteringDimension = dimensionDictionary.findByApiName(dimensionConfig.getFilteringDimensionApiName());
        String trueValue = dimensionConfig.getTrueValue();
        String falseValue = dimensionConfig.getFalseValue();

        Dimension baseGroupingDimension = dimensionDictionary.findByApiName(dimensionConfig.getGroupingBaseDimensionApiName());
        DefaultRegisteredLookupDimensionConfig groupingDimensionConfig;

        if (baseGroupingDimension instanceof RegisteredLookupDimension) {
            groupingDimensionConfig = new DefaultRegisteredLookupDimensionConfig(
                    (RegisteredLookupDimension) baseGroupingDimension,
                    UNUSED_PHYSICAL_NAME
            );
        } else if (baseGroupingDimension instanceof KeyValueStoreDimension) {
            groupingDimensionConfig = new DefaultRegisteredLookupDimensionConfig(
                    (KeyValueStoreDimension) baseGroupingDimension,
                    UNUSED_PHYSICAL_NAME
            );
        } else {
            groupingDimensionConfig = new DefaultRegisteredLookupDimensionConfig(
                    baseGroupingDimension,
                    UNUSED_PHYSICAL_NAME
            );
        }
        groupingDimension = new RegisteredLookupDimension(
                groupingDimensionConfig.withAddedLookupFunctions(
                        Collections.singletonList(
                                TagExtractionFunctionFactory.buildTagExtractionFunction(
                                        dimensionConfig.getTagValue(),
                                        trueValue,
                                        falseValue)))
        );

        DimensionField keyField = DefaultDimensionField.ID;
        trueRow = new DimensionRow(keyField, Collections.singletonMap(keyField, trueValue));
        falseRow = new DimensionRow(keyField, Collections.singletonMap(keyField, falseValue));

        rowMap = Stream.of(trueRow, falseRow)
                .collect(Collectors.toMap(DimensionRow::getKeyValue, Function.identity()));
        searchProvider = new MapSearchProvider(rowMap);
    }

    @Override
    public void setLastUpdated(DateTime lastUpdated) {
        throw new UnsupportedOperationException("Flag Proxy dimensions should never be updated directly");
    }

    @Override
    public String getApiName() {
        return dimensionConfig.getApiName();
    }

    @Override
    public String getDescription() {
        return dimensionConfig.getDescription();
    }

    @Override
    public DateTime getLastUpdated() {
        return filteringDimension.getLastUpdated();
    }

    @Override
    public LinkedHashSet<DimensionField> getDimensionFields() {
        return dimensionConfig.getFields();
    }

    @Override
    public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
        return dimensionConfig.getDefaultDimensionFields();
    }

    @Override
    public DimensionField getFieldByName(final String name) {
        return getKey();
    }

    @Override
    public SearchProvider getSearchProvider() {
        return searchProvider;
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
        return rowMap.computeIfAbsent(value, this::createEmptyDimensionRow);
    }

    @Override
    public DimensionField getKey() {
        return DefaultDimensionField.ID;
    }

    @Override
    public DimensionRow parseDimensionRow(Map<String, String> fieldNameValueMap) {
        if (!fieldNameValueMap.containsKey(getKey().getName())) {
            return falseRow;
        }
        return createEmptyDimensionRow(fieldNameValueMap.get(getKey().getName()));
    }

    @Override
    public DimensionRow createEmptyDimensionRow(String keyFieldValue) {
        if (rowMap.containsKey(keyFieldValue)) {
            return rowMap.get(keyFieldValue);
        }
        throw new IllegalArgumentException(String.format("Unparseable flag value: %s", keyFieldValue));
    }

    @Override
    public String getCategory() {
        return dimensionConfig.getCategory();
    }

    @Override
    public String getLongName() {
        return dimensionConfig.getLongName();
    }

    @Override
    public StorageStrategy getStorageStrategy() {
        return null;
    }

    @Override
    public int getCardinality() {
        return 3;
    }

    @Override
    public boolean isAggregatable() {
        return true;
    }

    public Dimension getGroupingDimension() {
        return groupingDimension;
    }

    public Dimension getFilteringDimension() {
        return filteringDimension;
    }
}
