// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.MapSearchProvider;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.TagExtractionFunctionFactory;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.FilterOperation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * Config bean describing a flag from tag dimension.
 */
public class FlagFromTagDimensionConfig extends DefaultRegisteredLookupDimensionConfig {

    public static final String DEFAULT_TRUE_VALUE = "true";
    public static final String DEFAULT_FALSE_VALUE = "false";

    public static final Set<FilterOperation> DEFAULT_POSITIVE_OPS = Stream.of(
            DefaultFilterOperation.in,
            DefaultFilterOperation.startswith,
            DefaultFilterOperation.contains,
            DefaultFilterOperation.eq
    ).collect(Collectors.collectingAndThen(
            Collectors.toSet(),
            Collections::unmodifiableSet
    ));

    public static final Set<FilterOperation> DEFAULT_NEGATIVE_OPS = Collections.unmodifiableSet(
            Collections.singleton(
                    DefaultFilterOperation.notin
            )
    );

    public static final FilterOperation DEFAULT_POSITIVE_INVERTED_FILTER_OPERATION = DefaultFilterOperation.eq;
    public static final FilterOperation DEFAULT_NEGATIVE_INVERTED_FILTER_OPERATION = DefaultFilterOperation.notin;

    // TODO in reality we only need a set of base extraction functions to extend and the name of the filtering dimension
    public final String filteringDimensionApiName;

    public final String tagValue;
    public final String trueValue;
    public final String falseValue;

    public final Set<FilterOperation> positiveOps;
    public final Set<FilterOperation> negativeOps;

    public final FilterOperation positiveInvertedFilterOperation;
    public final FilterOperation negativeInvertedFilterOperation;

    public final Map<String, DimensionRow> rowMap;

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
            @NotNull String filteringDimensionApiName,
            String tagValue,
            String trueValue,
            String falseValue,
            Set<FilterOperation> positiveOps,
            Set<FilterOperation> negativeOps,
            @NotNull FilterOperation positiveInvertedFilterOperation,
            @NotNull FilterOperation negativeInvertedFilterOperation
    ) {
        super(apiName, physicalName, description, longName, category, fields, defaultDimensionFields, keyValueStore, searchProvider, registeredLookupExtractionFns);
        this.filteringDimensionApiName = filteringDimensionApiName;
        this.tagValue = tagValue;
        this.trueValue = trueValue;
        this.falseValue = falseValue;
        this.positiveOps = Collections.unmodifiableSet(positiveOps);
        this.negativeOps = Collections.unmodifiableSet(negativeOps);
        this.positiveInvertedFilterOperation = positiveInvertedFilterOperation;
        this.negativeInvertedFilterOperation = negativeInvertedFilterOperation;
        this.rowMap = Collections.unmodifiableMap(rowMap);
    }

    // TODO dependencies on already created dimensions can be refactored out
    // TODO handle default values
    public static FlagFromTagDimensionConfig build(
            @NotNull DimensionName apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            @NotNull LinkedHashSet<DimensionField> fields,
            @NotNull LinkedHashSet<DimensionField> defaultDimensionFields,
            List<ExtractionFunction> baseExtractionFunctions,
            @NotNull String filteringDimensionApiName,
            String tagValue,
            String trueValue,
            String falseValue,
            Set<FilterOperation> positiveOps,
            Set<FilterOperation> negativeOps,
            @NotNull FilterOperation positiveInvertedFilterOperation,
            @NotNull FilterOperation negativeInvertedFilterOperation
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

        // build kvs
        Map<String, String> kvsTrueRow = new HashMap<>();
        kvsTrueRow.put(keyField.getName(), trueValue);
        Map<String, String> kvsFalseRow = new HashMap<>();
        kvsFalseRow.put(keyField.getName(), falseValue);

        KeyValueStore kvs = MapStoreManager.getInstance(physicalName);
        ObjectMapper mapper = new ObjectMappersSuite().getMapper();

        try {
            kvs.put(
                    DimensionStoreKeyUtils.getRowKey(keyField.getName(), trueValue),
                    mapper.writeValueAsString(kvsTrueRow)
            );
            kvs.put(
                    DimensionStoreKeyUtils.getRowKey(keyField.getName(), falseValue),
                    mapper.writeValueAsString(kvsFalseRow)
            );
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    String.format(
                            "unable to serialize true and false values in json for dimension %s",
                            apiName.asName()
                    )
            );
        }

        // extend the base extraction functions with the regex extraction
        List<ExtractionFunction> groupingDimensionExtractionFunctions = new ArrayList<>(baseExtractionFunctions);
        groupingDimensionExtractionFunctions.addAll(
                TagExtractionFunctionFactory.buildTagExtractionFunction(
                        tagValue,
                        trueValue,
                        falseValue)
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
                filteringDimensionApiName,
                tagValue,
                trueValue,
                falseValue,
                positiveOps,
                negativeOps,
                positiveInvertedFilterOperation,
                negativeInvertedFilterOperation
        );
    }
}
