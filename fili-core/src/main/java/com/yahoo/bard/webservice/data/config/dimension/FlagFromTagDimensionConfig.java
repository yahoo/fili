// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ExtractionFunctionDimension;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.impl.MapSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.TagExtractionFunctionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * Config bean describing a flag from tag dimension.
 */
public class FlagFromTagDimensionConfig extends DefaultRegisteredLookupDimensionConfig {

    public static final String NULL_VALUE = "";
    public static final String DEFAULT_TRUE_VALUE = "true";
    public static final String DEFAULT_FALSE_VALUE = "false";

    // TODO in reality we only need a set of base extraction functions to extend and the name of the filtering dimension
    private final Dimension filteringDimensionApiName;
    private final RegisteredLookupDimension groupingBaseDimensionApiName;

    private final String tagValue;
    private final String trueValue;
    private final String falseValue;
    private final Map<String, DimensionRow> rowMap;

    /**
     * Configuration for a dimension that uses the presence of a tag as the basis for a true/false flag dimension.
     *
     * @param apiName  The API Name is the external, end-user-facing name for the dimension.
     * @param description  A description of the dimension and its meaning.
     * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
     * @param category  The Category is the external, end-user-facing category for the dimension.
     * @param filteringDimensionApiName  The name of the tag dimension used when filter-serializing
     * @param groupingBaseDimensionApiName  The name of the string based tag dimension used as the basis for group by
     * operations
     * @param tagValue  The value whose presence in the flag dimen
     * @param trueValue The value to use as 'true' for this flag
     * @param falseValue The value to use as 'false' for this flag
     */
    public FlagFromTagDimensionConfig(
            @NotNull DimensionName apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            @NotNull LinkedHashSet<DimensionField> fields,
            @NotNull LinkedHashSet<DimensionField> defaultDimensionFields,
            @NotNull KeyValueStore keyValueStore,
            @NotNull SearchProvider searchProvider,
            Map<String, DimensionRow> rowMap,
            @NotNull List<ExtractionFunction> registeredLookupExtractionFns,
            @NotNull Dimension filteringDimensionApiName,
            @NotNull RegisteredLookupDimension groupingBaseDimensionApiName,
            String tagValue,
            String trueValue,
            String falseValue
    ) {
        super(apiName, physicalName, description, longName, category, fields, defaultDimensionFields, keyValueStore, searchProvider, registeredLookupExtractionFns);
        this.filteringDimensionApiName = filteringDimensionApiName;
        this.groupingBaseDimensionApiName = groupingBaseDimensionApiName;
        this.tagValue = tagValue;
        this.trueValue = trueValue;
        this.falseValue = falseValue;
        this.rowMap = Collections.unmodifiableMap(rowMap);
    }

//    /**
//     * Configuration for a dimension that uses the presence of a tag as the basis for a true/false flag dimension.
//     * Uses the default true and false dimension values.
//     *
//     * @param apiName  The API Name is the external, end-user-facing name for the dimension.
//     * @param description  A description of the dimension and its meaning.
//     * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
//     * @param category  The Category is the external, end-user-facing category for the dimension.
//     * @param filteringDimensionApiName  The name of the tag dimension used when filter-serializing
//     * @param groupingBaseDimensionApiName  The name of the string based tag dimension used as the basis for group by
//     * operations
//     * @param tagValue  The value whose presence in the flag dimen
//     */
//    public FlagFromTagDimensionConfig(
//            @NotNull DimensionName apiName,
//            String description,
//            String longName,
//            String category,
//            @NotNull String filteringDimensionApiName,
//            @NotNull String groupingBaseDimensionApiName,
//            String tagValue
//    ) {
//        this(
//                apiName,
//                description,
//                longName,
//                category,
//                filteringDimensionApiName,
//                groupingBaseDimensionApiName,
//                tagValue,
//                DEFAULT_TRUE_VALUE,
//                DEFAULT_FALSE_VALUE
//        );
//    }

    public Dimension getFilteringDimension() {
        return filteringDimensionApiName;
    }

    public RegisteredLookupDimension getGroupingDimension() {
        return groupingBaseDimensionApiName;
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

    public Map<String, DimensionRow> getRowMap() {
        return rowMap;
    }

    // TODO dependencies on already created dimensions can be refactored out
    public static FlagFromTagDimensionConfig build(
            @NotNull DimensionName apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            @NotNull LinkedHashSet<DimensionField> fields,
            @NotNull LinkedHashSet<DimensionField> defaultDimensionFields,
            @NotNull String filteringDimensionApiName,
            @NotNull String groupingBaseDimensionApiName,
            String tagValue,
            String trueValue,
            String falseValue,
            DimensionDictionary dimensionDictionary
    ) {
        // build search provider
        DimensionField keyField = fields.stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        String.format(
                                "Dimension %s has no fields",
                                apiName
                        )
                ));
        DimensionRow trueRow = new DimensionRow(keyField, Collections.singletonMap(keyField, trueValue));
        DimensionRow falseRow = new DimensionRow(keyField, Collections.singletonMap(keyField, falseValue));

        Map<String, DimensionRow> rowMap = Stream.of(trueRow, falseRow)
                .collect(Collectors.toMap(DimensionRow::getKeyValue, Function.identity()));
        SearchProvider searchProvider = new MapSearchProvider(rowMap);

        KeyValueStore kvs = MapStoreManager.getInstance(physicalName);
        kvs.put(keyField.getName(), trueValue);
        kvs.put(keyField.getName(), falseValue);

        Dimension filteringDimension = dimensionDictionary.findByApiName(filteringDimensionApiName);

        Dimension baseGroupingDimension = dimensionDictionary.findByApiName(
                groupingBaseDimensionApiName
        );
        DefaultRegisteredLookupDimensionConfig groupingDimensionConfig;

        List<ExtractionFunction> groupingDimensionExtractionFunctions = new ArrayList<>();

        // if present, pull off extraction functions into a list of extraction functions
        if (baseGroupingDimension instanceof ExtractionFunctionDimension) {
            groupingDimensionExtractionFunctions.addAll(
                    ((ExtractionFunctionDimension) baseGroupingDimension).getExtractionFunction()
                            .map(fn ->
                                    fn instanceof CascadeExtractionFunction ?
                                            ((CascadeExtractionFunction) fn).getExtractionFunctions() :
                                            Collections.singletonList(fn)
                            )
                            .orElse(Collections.emptyList())
            );
        }

        if (baseGroupingDimension instanceof KeyValueStoreDimension) {
            groupingDimensionConfig = new DefaultRegisteredLookupDimensionConfig(
                    (KeyValueStoreDimension) baseGroupingDimension,
                    physicalName
            );
        } else {
            groupingDimensionConfig = new DefaultRegisteredLookupDimensionConfig(
                    baseGroupingDimension::getApiName,
                    physicalName,
                    baseGroupingDimension.getDescription(),
                    baseGroupingDimension.getLongName(),
                    baseGroupingDimension.getCategory(),
                    baseGroupingDimension.getDimensionFields(),
                    baseGroupingDimension.getDefaultDimensionFields(),
                    MapStoreManager.getInstance(baseGroupingDimension.getApiName()),
                    baseGroupingDimension.getSearchProvider(),
                    Collections.EMPTY_LIST
            );
        }

        groupingDimensionExtractionFunctions.addAll(
                TagExtractionFunctionFactory.buildTagExtractionFunction(
                        tagValue,
                        trueValue,
                        falseValue)
        );

        RegisteredLookupDimension groupingDimension = new RegisteredLookupDimension(
                groupingDimensionConfig.withAddedLookupFunctions(groupingDimensionExtractionFunctions)
        );

        return new FlagFromTagDimensionConfig(
                apiName,
                physicalName,
                description,
                longName,
                category,
                fields,
                defaultDimensionFields,
                kvs,
                searchProvider,
                rowMap,
                groupingDimensionExtractionFunctions,
                filteringDimension,
                groupingDimension,
                tagValue,
                trueValue,
                falseValue
        );
    }
}
