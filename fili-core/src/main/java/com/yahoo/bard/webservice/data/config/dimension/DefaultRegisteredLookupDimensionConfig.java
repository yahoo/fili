// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * A Default Registered Lookup Dimension holds all of the information needed to construct a Registered Lookup Dimension.
 */
public class DefaultRegisteredLookupDimensionConfig extends DefaultKeyValueStoreDimensionConfig
        implements RegisteredLookupDimensionConfig {

    private final List<ExtractionFunction> registeredLookupExtractionFns;

    /**
     * Construct a new {@code RegisteredLookupDefaultDimensionConfig} with the specified a dimension's metadata, the
     * dimension join facilities for that dimension({@link KeyValueStore} and {@link SearchProvider}), and a list of
     * registered lookup extraction function of that dimension.
     *
     * @param apiName  An external and end-user-facing Webservice API name for the dimension
     * @param physicalName  The dimension name as shown in Druid
     * @param description  A description of the dimension and its meaning.
     * @param longName  An external and end-uder-facing name for the dimension, this could be used as UI name for the
     * this dimension
     * @param category  The group name for a set of similar dimensions
     * @param fields  A set of dimension fields for this dimension
     * @param defaultDimensionFields  The default set of fields for this dimension to be shown in the response
     * @param keyValueStore  The key value store holding dimension row data used for dimension join
     * @param searchProvider  A dimension join facility for searching through dimension metadata
     * @param registeredLookupExtractionFns  A list of registered lookup extraction functions used to perform lookups
     */
    public DefaultRegisteredLookupDimensionConfig(
            @NotNull DimensionName apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            @NotNull LinkedHashSet<DimensionField> fields,
            @NotNull LinkedHashSet<DimensionField> defaultDimensionFields,
            @NotNull KeyValueStore keyValueStore,
            @NotNull SearchProvider searchProvider,
            @NotNull List<ExtractionFunction> registeredLookupExtractionFns
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
                searchProvider
        );
        this.registeredLookupExtractionFns = Collections.unmodifiableList(registeredLookupExtractionFns);
    }

    /**
     * Construct a new {@code RegisteredLookupDefaultDimensionConfig} with the specified a dimension's metadata, the
     * dimension join facilities for that dimension({@link KeyValueStore} and {@link SearchProvider}), and a list of
     * registered lookup extraction function of that dimension.
     * <p>
     * This constructor initializes so that all dimension fields and default dimension fields are the same.
     *
     * @param apiName  An external and end-user-facing Webservice API name for the dimension
     * @param physicalName  The dimension name as shown in Druid
     * @param description  A description of the dimension and its meaning.
     * @param longName  An external and end-uder-facing name for the dimension, this could be used as UI name for the
     * this dimension
     * @param category  The group name for a set of similar dimensions
     * @param fields  A set of dimension fields for this dimension
     * @param keyValueStore  The key value store holding dimension row data used for dimension join
     * @param searchProvider  A dimension join facility for searching through dimension metadata
     * @param registeredLookupExtractionFns  A list of registered lookup extraction functions used to perform lookups
     */
    public DefaultRegisteredLookupDimensionConfig(
            @NotNull DimensionName apiName,
            String physicalName,
            String description,
            String longName,
            String category,
            @NotNull LinkedHashSet<DimensionField> fields,
            @NotNull KeyValueStore keyValueStore,
            @NotNull SearchProvider searchProvider,
            @NotNull List<ExtractionFunction> registeredLookupExtractionFns
    ) {
        this(
                apiName,
                physicalName,
                description,
                longName,
                category,
                fields,
                fields,
                keyValueStore,
                searchProvider,
                registeredLookupExtractionFns
        );
    }

    /**
     * Construct a DefaultKeyValueStoreDimensionConfig instance from a RegisteredLookupDimension and a physical
     * column name.
     *
     * @param dimension  The dimension whose config should be copied.
     * @param physicalName  The internal, physical name for the dimension.
     */
    public DefaultRegisteredLookupDimensionConfig(
            RegisteredLookupDimension dimension,
            String physicalName
    ) {
        this(
                (DimensionName) dimension::getApiName,
                physicalName,
                dimension.getDescription(),
                dimension.getLongName(),
                dimension.getCategory(),
                dimension.getDimensionFields(),
                dimension.getDefaultDimensionFields(),
                dimension.getKeyValueStore(),
                dimension.getSearchProvider(),
                dimension.getRegisteredLookupExtractionFns()
        );
    }

    /**
     * Construct a DefaultKeyValueStoreDimensionConfig instance from a KeyValueStoreDimension and physical column name.
     *
     *
     * @param dimension  The dimension whose config should be copied.
     * @param physicalName  The internal, physical name for the dimension.
     */
    public DefaultRegisteredLookupDimensionConfig(
            KeyValueStoreDimension dimension,
            String physicalName
    ) {
        this(
                (DimensionName) dimension::getApiName,
                physicalName,
                dimension.getDescription(),
                dimension.getLongName(),
                dimension.getCategory(),
                dimension.getDimensionFields(),
                dimension.getDefaultDimensionFields(),
                dimension.getKeyValueStore(),
                dimension.getSearchProvider(),
                Collections.EMPTY_LIST
        );
    }

    /**
     * Construct a DefaultKeyValueStoreDimensionConfig instance from a Dimension and physical column name.
     *
     *
     * @param dimension  The dimension whose config should be copied.
     * @param physicalName  The internal, physical name for the dimension.
     */
    public DefaultRegisteredLookupDimensionConfig(
            Dimension dimension,
            String physicalName
    ) {
        this(
                (DimensionName) dimension::getApiName,
                physicalName,
                dimension.getDescription(),
                dimension.getLongName(),
                dimension.getCategory(),
                dimension.getDimensionFields(),
                dimension.getDefaultDimensionFields(),
                null,
                dimension.getSearchProvider(),
                Collections.EMPTY_LIST
        );
    }
    /**
     * Construct a copy of this config with additional extraction functions appended.
     *
     * @param additionalFunctions  ExtractionFunctions to add to the end of the function list.
     *
     * @return  A modified copy with additional extraction functions.
     */
    public DefaultRegisteredLookupDimensionConfig withAddedLookupFunctions(
            List<ExtractionFunction> additionalFunctions
    ) {
        return new DefaultRegisteredLookupDimensionConfig(
                this::getApiName,
                getPhysicalName(),
                getDescription(),
                getLongName(),
                getCategory(),
                getFields(),
                getDefaultDimensionFields(),
                getKeyValueStore(),
                getSearchProvider(),
                Stream.of(getRegisteredLookupExtractionFns(), additionalFunctions)
                        .flatMap(List::stream)
                        .collect(Collectors.toList())
        );
    }

    @Override
    public List<ExtractionFunction> getRegisteredLookupExtractionFns() {
        return registeredLookupExtractionFns;
    }
}
