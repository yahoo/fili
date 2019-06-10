// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension;
import com.yahoo.bard.webservice.data.dimension.impl.MapSearchProvider;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.TagExtractionFunctionFactory;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.FilterOperation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
     * Constructor. Private to ensure use of the Builder.
     *
     * @param apiName the API name of the FlagFromTag dimension
     * @param physicalName the physical name of the column the FlagFromTag dimension will GROUP on
     * @param description the description of the FlagFromTag dimension
     * @param longName the long name (UI name) of the FlagFromTag dimension
     * @param category the category of the FlagFromTag dimension
     * @param fields the set of fields for the FlagFromTag dimension
     * @param defaultDimensionFields the default set of fields to annotate onto response data
     * @param keyValueStore the key value store backing this FlagFromTag dimension
     * @param searchProvider the search provider backing this FlagFromTag dimension
     * @param rowMap mapping of truth value to dimension row for that truth value
     * @param registeredLookupExtractionFns the extraction functions, including the functions to transform the tag
     * value into the flag value
     * @param filteringDimensionApiName the API name of the dimension which filter operations will be transformed to
     * target
     * @param tagValue the flag value whose presence will determine the flag value
     * @param trueValue the value to be used if the flag value is present
     * @param falseValue the value to be used if the flag value is <b>not</b> present
     * @param positiveOps the allowed set of positive filter operations on the FlagFromTag dimension
     * @param negativeOps the allowed set of negative filter operations on the FlagFromTag dimension
     * @param positiveInvertedFilterOperation the filter operation to use if a negative filter on the FlagFromTag
     * dimension needs to be inverted.
     * @param negativeInvertedFilterOperation the filter operation to use if a positive filter on the FlagFromTag
     * dimension needs to be inverted.
     */
    private FlagFromTagDimensionConfig(
            DimensionName apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            LinkedHashSet<DimensionField> fields,
            LinkedHashSet<DimensionField> defaultDimensionFields,
            KeyValueStore keyValueStore,
            SearchProvider searchProvider,
            Map<String, DimensionRow> rowMap,
            List<ExtractionFunction> registeredLookupExtractionFns,
            String filteringDimensionApiName,
            String tagValue,
            String trueValue,
            String falseValue,
            Set<FilterOperation> positiveOps,
            Set<FilterOperation> negativeOps,
            FilterOperation positiveInvertedFilterOperation,
            FilterOperation negativeInvertedFilterOperation
    ) {
        super(
                apiName,
                physicalName,
                description,
                longName,
                category,
                fields,
                defaultDimensionFields,
                keyValueStore,
                searchProvider,
                registeredLookupExtractionFns
        );
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

    @Override
    public Class getType() {
        return FlagFromTagDimension.class;
    }

    /**
     * Builder for a FlagFromTagDimensionConfig.
     */
    public static class Builder {
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

        public static final Set<FilterOperation> DEFAULT_NEGATIVE_OPS
                = Collections.singleton(DefaultFilterOperation.notin);

        public static final FilterOperation DEFAULT_POSITIVE_INVERTED_FILTER_OPERATION = DefaultFilterOperation.eq;
        public static final FilterOperation DEFAULT_NEGATIVE_INVERTED_FILTER_OPERATION = DefaultFilterOperation.notin;

        private final DimensionName apiName;
        private final String physicalName;
        private final String description;
        private final String longName;
        private final String category;
        private final String filteringDimensionApiName;
        private final String tagValue;

        private LinkedHashSet<DimensionField> fields
                = Stream.of(DefaultDimensionField.ID).collect(Collectors.toCollection(LinkedHashSet::new));
        private LinkedHashSet<DimensionField> defaultDimensionFields
                = Stream.of(DefaultDimensionField.ID).collect(Collectors.toCollection(LinkedHashSet::new));

        private List<ExtractionFunction> baseExtractionFunctions = new ArrayList<>();
        private String trueValue = DEFAULT_TRUE_VALUE;
        private String falseValue = DEFAULT_FALSE_VALUE;
        private Set<FilterOperation> positiveOps = DEFAULT_POSITIVE_OPS;
        private Set<FilterOperation> negativeOps = DEFAULT_NEGATIVE_OPS;
        private FilterOperation positiveInvertedFilterOperation = DEFAULT_POSITIVE_INVERTED_FILTER_OPERATION;
        private FilterOperation negativeInvertedFilterOperation = DEFAULT_NEGATIVE_INVERTED_FILTER_OPERATION;

        /**
         * Constructor. Takes as parameters the required parameters to build a {@link FlagFromTagDimensionConfig}. Any
         * other parameters can be set through the named setter methods
         *
         * @param apiName the API name of the FlagFromTag dimension
         * @param physicalName the physical name of the column the FlagFromTag dimension will GROUP on
         * @param description the description of the FlagFromTag dimension
         * @param longName the long name (UI name) of the FlagFromTag dimension
         * @param category the category of the FlagFromTag dimension
         * @param filteringDimensionApiName the API name of dimension to be used to build druid filters for the
         * FlagFromTag dimension.
         * @param tagValue The tag value the FlagFromTag dimension is based on.
         */
        public Builder(
                @NotNull DimensionName apiName,
                @NotNull String physicalName,
                @NotNull String description,
                @NotNull String longName,
                @NotNull String category,
                @NotNull String filteringDimensionApiName,
                @NotNull String tagValue
        ) {
            this.apiName = java.util.Objects.requireNonNull(apiName);
            this.physicalName = java.util.Objects.requireNonNull(physicalName);
            this.description = java.util.Objects.requireNonNull(description);
            this.longName = java.util.Objects.requireNonNull(longName);
            this.category = java.util.Objects.requireNonNull(category);
            this.filteringDimensionApiName = java.util.Objects.requireNonNull(filteringDimensionApiName);
            this.tagValue = java.util.Objects.requireNonNull(tagValue);
        }

        /**
         * Sets the dimensions fields and the default show fields. The first element in the set MUST be the key field
         * of the FlagFromTag dimension.
         *
         * @param fields the set of fields for the FlagFromTag dimension
         * @return this builder.
         */
        public Builder fields(LinkedHashSet<DimensionField> fields) {
            this.fields = new LinkedHashSet<>(fields);
            this.defaultDimensionFields = this.fields;
            return this;
        }

        /**
         * Sets the dimensions fields and the default show fields. The first element in the set MUST be the key field
         * of the FlagFromTag dimension. defaultShowFields MUST be a subset of fields.
         *
         * @param fields the set of fields for the FlagFromTag dimension
         * @param defaultShowFields the default set of fields to annotate onto response data
         * @return this builder.
         */
        public Builder fields(
                LinkedHashSet<DimensionField> fields,
                LinkedHashSet<DimensionField> defaultShowFields
        ) {
            if (!defaultShowFields.stream().allMatch(fields::contains)) {
                throw new IllegalArgumentException(
                        String.format(
                            "Attempted to configure FlagFromTag dimension with default show fields %s that are NOT " +
                                    "a subset of the configured dimension fields %s",
                                String.join(
                                        ", ",
                                        defaultShowFields.stream()
                                                .map(DimensionField::getName)
                                                .collect(Collectors.toList())
                                ),
                                String.join(
                                        ", ",
                                        fields.stream()
                                                .map(DimensionField::getName)
                                                .collect(Collectors.toList())
                                )
                        )
                );
            }

            this.fields = new LinkedHashSet<>(fields);
            this.defaultDimensionFields = new LinkedHashSet<>(defaultShowFields);
            return this;
        }

        /**
         * Sets the base extraction functions of the FlagFromTag dimension. The FlagFromTag dimension will always
         * append 2 extraction functions which extract the flag and transform it into one of the configured truth
         * values based on the existence of the flag in the list of tags.
         *
         * @param extractionFunctions the extraction functions to set.
         * @return this builder
         */
        public Builder extractionFunctions(List<ExtractionFunction> extractionFunctions) {
            baseExtractionFunctions = new ArrayList<>(extractionFunctions);
            return this;
        }

        /**
         * <b>Appends</b> an extraction function to the list of base extraction functions. If the extraction function
         * is a {@link CascadeExtractionFunction}, it will be flattened into its underlying extraction functions before
         * being appended to the list of base extraction functions. The FlagFromTag dimension will always append
         * 2 extraction functions which extract the flag and transform it into one of the configured truth values based
         * on the existence of the flag in the list of tags.
         *
         * @param extractionFunction the extraction function to append
         * @return this builder
         */
        public Builder addExtractionFunction(ExtractionFunction extractionFunction) {
            java.util.Objects.requireNonNull(extractionFunction);

            baseExtractionFunctions.addAll(
                    extractionFunction instanceof CascadeExtractionFunction ?
                            new ArrayList<>(((CascadeExtractionFunction) extractionFunction).getExtractionFunctions())
                            : Collections.singletonList(extractionFunction)
            );
            return this;
        }

        /**
         * Sets the value that represents true in the FlagForTag dimension. If the flag value is found when grouping
         * or filtering it will get converted to this value. By default this value is "true".
         *
         * @param trueValue the true value to set.
         * @return this builder
         */
        public Builder trueValue(String trueValue) {
           this.trueValue = java.util.Objects.requireNonNull(trueValue);
           return this;
        }


        /**
         * Sets the value that represents false in the FlagForTag dimension. If the flag value is not found when
         * grouping or filtering it will get converted to this value. By default this value is "false".
         *
         * @param falseValue the false value to set.
         * @return this builder
         */
        public Builder falseValue(String falseValue) {
           this.falseValue = java.util.Objects.requireNonNull(falseValue);
           return this;
        }

        /**
         * Sets the set of accepted "positive" filter operations (e.g. eq) on the FlagFromTag dimension.
         * Cannot be empty.
         *
         * @param positiveOps the accepted positive operations.
         * @return this builder
         * @throws IllegalStateException if the input collection is empty
         */
        public Builder positiveOps(Collection<FilterOperation> positiveOps) {
            if (positiveOps.isEmpty()) {
                throw new IllegalArgumentException(
                        "attempted to configure FlagFromTag dimension with an empty set of positive filter operations"
                );
            }
            this.positiveOps = new HashSet<>(positiveOps);
            return this;
        }

        /**
         * Sets the set of accepted "negative" filter operations (e.g. noteq) on the FlagFromTag dimension. Cannot
         * be empty.
         *
         * @param negativeOps the accepted negative operations.
         * @return this builder
         * @throws IllegalStateException if the input collection is empty
         */
        public Builder negativeOps(Collection<FilterOperation> negativeOps) {
            if (negativeOps.isEmpty()) {
                throw new IllegalArgumentException(
                        "attempted to configure FlagFromTag dimension with an empty set of negative filter operations"
                );
            }
            this.negativeOps = new HashSet<>(negativeOps);
            return this;
        }

        /**
         * Sets the positive filter operation to use in the case that a negative filter operation needs to be inverted.
         *
         * @param positiveInvertedFilterOperation the positive filter operation
         * @return  this builder
         */
        public Builder positiveInvertedFilterOperation(FilterOperation positiveInvertedFilterOperation) {
            this.positiveInvertedFilterOperation = java.util.Objects.requireNonNull(positiveInvertedFilterOperation);
            return this;
        }

        /**
         * Sets the negative filter operation to use in the case that a positive filter operation needs to be inverted.
         *
         * @param negativeInvertedFilterOperation the negative filter operation
         * @return  this builder
         */
        public Builder negativeInvertedFilterOperation(FilterOperation negativeInvertedFilterOperation) {
            this.negativeInvertedFilterOperation = java.util.Objects.requireNonNull(negativeInvertedFilterOperation);
            return this;
        }

        /**
         * Builds the config.
         *
         * @return  a {@link FlagFromTagDimensionConfig}
         */
        public FlagFromTagDimensionConfig build() {
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
                            falseValue
                    )
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
}
