// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;

import org.joda.time.DateTime;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Build a Flag dimension with a simple true and false filter corresponding to a multivalued 'tag' dimension.
 */
//@JsonSerialize(using = FlagFromTagDimensionSpec.class)
public class FlagFromTagDimension extends RegisteredLookupDimension {

    private final FlagFromTagDimensionConfig dimensionConfig;

    private final Map<String, DimensionRow> rowMap;

    private final RegisteredLookupDimension groupingDimension;
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
    public FlagFromTagDimension(FlagFromTagDimensionConfig config) {
        super(config);
        this.dimensionConfig = config;
        this.filteringDimension = config.getFilteringDimension();
        this.groupingDimension = config.getGroupingDimension();
        tagValue = dimensionConfig.getTagValue();
        trueValue = dimensionConfig.getTrueValue();
        falseValue = dimensionConfig.getFalseValue();
        this.rowMap = Collections.unmodifiableMap(config.getRowMap());

//        Dimension baseGroupingDimension = dimensionDictionary.findByApiName(
//                dimensionConfig.getGroupingBaseDimensionApiName()
//        );
//        DefaultRegisteredLookupDimensionConfig groupingDimensionConfig;
//
//        List<ExtractionFunction> groupingDimensionExtractionFunctions = new ArrayList<>();
//
//        // if present, pull off extraction functions into a list of extraction functions
//        if (baseGroupingDimension instanceof ExtractionFunctionDimension) {
//            groupingDimensionExtractionFunctions.addAll(
//                    ((ExtractionFunctionDimension) baseGroupingDimension).getExtractionFunction()
//                    .map(fn ->
//                            fn instanceof CascadeExtractionFunction ?
//                                ((CascadeExtractionFunction) fn).getExtractionFunctions() :
//                                Collections.singletonList(fn)
//                    )
//                    .orElse(Collections.emptyList())
//            );
//        }
//
//        if (baseGroupingDimension instanceof KeyValueStoreDimension) {
//            groupingDimensionConfig = new DefaultRegisteredLookupDimensionConfig(
//                    (KeyValueStoreDimension) baseGroupingDimension,
//                    UNUSED_PHYSICAL_NAME
//            );
//        } else {
//            groupingDimensionConfig = new DefaultRegisteredLookupDimensionConfig(
//                    baseGroupingDimension::getApiName,
//                    UNUSED_PHYSICAL_NAME,
//                    baseGroupingDimension.getDescription(),
//                    baseGroupingDimension.getLongName(),
//                    baseGroupingDimension.getCategory(),
//                    baseGroupingDimension.getDimensionFields(),
//                    baseGroupingDimension.getDefaultDimensionFields(),
//                    MapStoreManager.getInstance(flagDimensionConfig.getApiName()),
//                    baseGroupingDimension.getSearchProvider(),
//                    Collections.EMPTY_LIST
//            );
//        }
//
//        groupingDimensionExtractionFunctions.addAll(
//                TagExtractionFunctionFactory.buildTagExtractionFunction(
//                        dimensionConfig.getTagValue(),
//                        trueValue,
//                        falseValue)
//        );
//
//        groupingDimension = new RegisteredLookupDimension(
//                groupingDimensionConfig.withAddedLookupFunctions(groupingDimensionExtractionFunctions)
//        );
//
//        DimensionField keyField = DefaultDimensionField.ID;
//        trueRow = new DimensionRow(keyField, Collections.singletonMap(keyField, trueValue));
//        falseRow = new DimensionRow(keyField, Collections.singletonMap(keyField, falseValue));
//
//        rowMap = Stream.of(trueRow, falseRow)
//                .collect(Collectors.toMap(DimensionRow::getKeyValue, Function.identity()));
//        searchProvider = new MapSearchProvider(rowMap);
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

    public RegisteredLookupDimension getGroupingDimension() {
        return groupingDimension;
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
