// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.*;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Dimension template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "apiName": "REGION_ISO_CODE",
 *          "longName": "wiki regionIsoCode",
 *          "description": "Iso Code of the region to which the wiki page belongs",
 *          "fields": "default"
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultDimensionTemplate implements DimensionTemplate {

    private final String apiName;
    private final String description;
    private final String longName;
    private final String category;
    private final List<DimensionFieldInfoTemplate> fieldList;

    /**
     * Constructor used by json parser.
     *
     * @param apiName json property apiName
     * @param description json property description
     * @param longName json property longName
     * @param category json property category
     * @param fieldList json property fields
     */
    @JsonCreator
    public DefaultDimensionTemplate(
            @NotNull @JsonProperty("apiName") String apiName,
            @JsonProperty("description") String description,
            @JsonProperty("longName") String longName,
            @JsonProperty("category") String category,
            @JsonProperty("fields") List<DimensionFieldInfoTemplate> fieldList
    ) {
        this.apiName = apiName;
        this.description = (Objects.isNull(description) ? "" : description);
        this.longName = (Objects.isNull(longName) ? apiName : longName);
        this.category = (Objects.isNull(category) ? Dimension.DEFAULT_CATEGORY : category);
        this.fieldList = fieldList;
    }

    /**
     * Get dimensions info.
     */
    @Override
    public String getApiName() {
        return this.apiName;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String getLongName() {
        return this.longName;
    }

    @Override
    public String getCategory() {
        return this.category;
    }

    @Override
    public String toString() {
        return this.getApiName();
    }

    @Override
    public LinkedHashSet<DimensionField> getFields() {
        sortFields(this.fieldList);
        return this.fieldList.stream()
                .map(DimensionFieldInfoTemplate::build)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public DimensionConfig build() {
        return new DefaultKeyValueStoreDimensionConfig(
                () -> (getApiName()),
                getApiName(),
                getDescription(),
                getLongName(),
                getCategory(),
                getFields(),
                getDefaultKeyValueStore(),
                getDefaultSearchProvider()
        );
    }

    /**
     * Lazily provide a KeyValueStore for this store name.
     *
     * @return A KeyValueStore instance
     */
    private KeyValueStore getDefaultKeyValueStore() {
        return MapStoreManager.getInstance(getApiName());
    }

    /**
     * Lazily create a Scanning Search Provider for this provider name.
     *
     * @return A Scanning Search Provider for the provider name.
     */
    private SearchProvider getDefaultSearchProvider() {
        return ScanSearchProviderManager.getInstance(getApiName());
    }
}
