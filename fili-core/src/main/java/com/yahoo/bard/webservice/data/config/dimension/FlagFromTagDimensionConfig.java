// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;

import java.util.Collections;
import java.util.LinkedHashSet;

import javax.validation.constraints.NotNull;

/**
 * Config bean describing a flag from tag dimension.
 */
public class FlagFromTagDimensionConfig implements DimensionConfig {

    public static final String NULL_VALUE = "";
    public static final String DEFAULT_TRUE_VALUE = "true";
    public static final String DEFAULT_FALSE_VALUE = "false";

    private final DimensionName apiName;
    private final String description;
    private final String longName;
    private final String category;

    private final String filteringDimensionApiName;
    private final String groupingBaseDimensionApiName;

    private final String tagValue;
    private final String trueValue;
    private final String falseValue;


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
            String description,
            String longName,
            String category,
            @NotNull String filteringDimensionApiName,
            @NotNull String groupingBaseDimensionApiName,
            String tagValue,
            String trueValue,
            String falseValue
    ) {
        this.apiName = apiName;
        this.description = description;
        this.longName = longName;
        this.category = category;

        this.filteringDimensionApiName = filteringDimensionApiName;
        this.groupingBaseDimensionApiName = groupingBaseDimensionApiName;
        this.tagValue = tagValue;
        this.trueValue = trueValue;
        this.falseValue = falseValue;
    }

    /**
     * Configuration for a dimension that uses the presence of a tag as the basis for a true/false flag dimension.
     * Uses the default true and false dimension values.
     *
     * @param apiName  The API Name is the external, end-user-facing name for the dimension.
     * @param description  A description of the dimension and its meaning.
     * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
     * @param category  The Category is the external, end-user-facing category for the dimension.
     * @param filteringDimensionApiName  The name of the tag dimension used when filter-serializing
     * @param groupingBaseDimensionApiName  The name of the string based tag dimension used as the basis for group by
     * operations
     * @param tagValue  The value whose presence in the flag dimen
     */
    public FlagFromTagDimensionConfig(
            @NotNull DimensionName apiName,
            String description,
            String longName,
            String category,
            @NotNull String filteringDimensionApiName,
            @NotNull String groupingBaseDimensionApiName,
            String tagValue
    ) {
        this(
                apiName,
                description,
                longName,
                category,
                filteringDimensionApiName,
                groupingBaseDimensionApiName,
                tagValue,
                DEFAULT_TRUE_VALUE,
                DEFAULT_FALSE_VALUE
        );
    }
    @Override
    public String getApiName() {
        return apiName.asName();
    }

    @Override
    public String getLongName() {
        return longName;
    }

    @Override
    public String getCategory() {
        return category;
    }

    @Override
    public String getPhysicalName() {
        return null;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public StorageStrategy getStorageStrategy() {
        return null;
    }

    @Override
    public LinkedHashSet<DimensionField> getFields() {
        return new LinkedHashSet<>(Collections.singleton(DefaultDimensionField.ID));
    }

    @Override
    public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
        return new LinkedHashSet<>(Collections.singleton(DefaultDimensionField.ID));
    }

    @Override
    public KeyValueStore getKeyValueStore() {
        return null;
    }

    @Override
    public SearchProvider getSearchProvider() {
        return null;
    }

    public String getFilteringDimensionApiName() {
        return filteringDimensionApiName;
    }

    public String getGroupingBaseDimensionApiName() {
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
}
