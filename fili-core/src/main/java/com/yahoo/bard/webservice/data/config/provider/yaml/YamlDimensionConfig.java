// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProviderManager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Dimension configuration.
 *
 * FIXME: Need to implement the following methods:
 *  * getKeyValueStore
 *  * getSearchProvider
 *  * getType
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class YamlDimensionConfig implements DimensionConfig {

    private static final Logger LOG = LoggerFactory.getLogger(YamlDimensionConfig.class);

    protected String dimensionName;
    protected String apiName;
    protected String physicalName;
    protected String longName;
    protected String category;
    protected String description;

    protected List<String> dimensionFieldNames = null;
    protected List<String> defaultDimensionFieldNames = null;
    protected LinkedHashSet<DimensionField> allDimensionFields = null;
    protected LinkedHashSet<DimensionField> defaultDimensionFields = null;
    protected KeyValueStore keyValueStore;
    protected SearchProvider searchProvider;
    protected Boolean aggregatable = true;

    /**
     * Construct the dimension configuration.
     *
     * @param physicalName the physical name
     * @param longName the long name
     * @param category the category
     * @param description the description
     * @param dimensionFields the dimension fields
     * @param defaultDimensionFields the default dimension fields
     * @param keyValueStore the keyvalue store
     * @param searchProvider the search provider
     * @param aggregatable true if the dimension is aggregatable
     */
    @JsonCreator
    public YamlDimensionConfig(
            @JsonProperty("physical_name") String physicalName,
            @JsonProperty("long_name") String longName,
            @JsonProperty("category") String category,
            @JsonProperty("description") String description,
            @JsonProperty("fields") String[] dimensionFields,
            @JsonProperty("default_fields") String[] defaultDimensionFields,
            @JsonProperty("key_value_store") String keyValueStore,
            @JsonProperty("search_provider") String searchProvider,
            @JsonProperty("aggregatable") Boolean aggregatable
    ) {
        this.physicalName = physicalName;
        this.longName = longName;
        this.category = category;
        this.description = description;

        // Set and validate dimension fields
        if (dimensionFields != null && dimensionFields.length > 0) {
            dimensionFieldNames = Arrays.asList(dimensionFields);
            if (!isUniqueList(dimensionFieldNames)) {
                throw new RuntimeException("Error: must provide unique list of dimension fields. Found: " + Arrays
                        .toString(
                        dimensionFields));
            }
        } else {
            LOG.info("No fields configured for dimension " + physicalName + "; will use defaults.");
        }

        // Set and validate default dimension fields
        if (defaultDimensionFields != null && defaultDimensionFields.length > 0) {
            defaultDimensionFieldNames = Arrays.asList(defaultDimensionFields);
            if (!isUniqueList(defaultDimensionFieldNames)) {
                throw new RuntimeException("Error: must provide unique list of default dimension fields. Found: " +
                        Arrays
                        .toString(defaultDimensionFields));
            }
        } else {
            LOG.info("No default fields configured for dimension " + physicalName + "; will use defaults.");
        }

        this.aggregatable = (aggregatable != null ? aggregatable : this.aggregatable);
    }

    /**
     * Set the API name.
     *
     * Intended to be used by Jackson deserializer
     *
     * @param apiName the API name
     */
    public void setApiName(String apiName) {
        this.apiName = apiName;

        // Set defaults, too
        if (this.physicalName == null) {
            this.physicalName = apiName;
        }
        if (this.longName == null) {
            this.longName = apiName;
        }

        if (this.description == null) {
            this.description = apiName;
        }
    }

    /**
     * Set the available dimension fields.
     *
     * There is a global list of dimension fields; this tells the dimensions about them.
     *
     * @param availableFields configured available dimension fields
     */
    public void setAvailableDimensionFields(Map<String, YamlDimensionFieldConfig> availableFields) {

        LinkedHashSet<String> allDimensionFieldNames = new LinkedHashSet<>();

        // If no dimension fields were specified, make all globally defined fields available.
        // Otherwise, use just the ones specified.
        if (dimensionFieldNames == null) {
            allDimensionFields = new LinkedHashSet<>(availableFields.values());
            allDimensionFieldNames.addAll(availableFields.keySet());
        } else {

            // You can't ask for a dimension field that doesn't exist in the global config
            if (!dimensionFieldNames.stream().allMatch(availableFields::containsKey)) {
                throw new RuntimeException("Asked for unconfigured dimension field; requested fields: " +
                        Arrays.toString(dimensionFieldNames.toArray()) +
                        "; available fields: " +
                        Arrays.toString(availableFields.keySet().toArray()));
            }

            allDimensionFieldNames.addAll(dimensionFieldNames);
            allDimensionFields = dimensionFieldNames
                    .stream()
                    .map(availableFields::get)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        // If no default fields were specified, use the default included fields from those
        // that are available for this dimension.
        // Otherwise, use the defaults that were specified.
        if (defaultDimensionFieldNames == null) {
            defaultDimensionFields = allDimensionFields
                    .stream()
                    .filter(d -> ((YamlDimensionFieldConfig) d).includedByDefault())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } else {

            // You can't ask for a default dimension field that isn't available, either
            if (!defaultDimensionFieldNames.stream().allMatch(allDimensionFieldNames::contains)) {
                throw new RuntimeException("Asked for unconfigured default dimension field; requested fields: " +
                        Arrays.toString(dimensionFieldNames.toArray()) +
                        "; available fields: " +
                        Arrays.toString(allDimensionFieldNames.toArray()));
            }

            defaultDimensionFields = defaultDimensionFieldNames
                    .stream()
                    .filter(availableFields::containsKey)
                    .map(availableFields::get)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public String getApiName() {
        return apiName;
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
    public String getDescription() {
        return description;
    }

    /**
     * Return all dimension fields.
     *
     * @return the dimension fields
     */
    @Override
    public LinkedHashSet<DimensionField> getFields() {

        if (allDimensionFields == null) {
            throw new RuntimeException("Dimension fields not available from configuration.");
        }

        return allDimensionFields;
    }

    /**
     * Return the default dimension fields.
     *
     * @return the default dimension fields
     */
    @Override
    public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
        if (defaultDimensionFields == null) {
            throw new RuntimeException("Dimension fields not available from configuration.");
        }

        return defaultDimensionFields;
    }

    @Override
    public boolean isAggregatable() {
        return aggregatable;
    }

    @Override
    public String getPhysicalName() {
        return physicalName;
    }

    @Override
    public KeyValueStore getKeyValueStore() {
        // return keyValueStore;
        // Not really implemented yet
        return MapStoreManager.getInstance(apiName);
    }

    // FIXME: not implemented yet
    @Override
    public SearchProvider getSearchProvider() {
        // return searchProvider;
        // Not really implemented yet
        return NoOpSearchProviderManager.getInstance(apiName);
    }

    /**
     * Return true if the passed list contains a unique collection of elements.
     *
     * @param elements array of elements
     * @param <E> type of element
     * @return true if the list has no duplicate elements
     */
    public static <E> boolean isUniqueList(List<E> elements) {
        return elements.size() == new HashSet<>(elements).size();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof YamlDimensionConfig)) {
            return false;
        }

        YamlDimensionConfig conf = (YamlDimensionConfig) other;

        return Objects.equals(dimensionName, conf.dimensionName) &&
                Objects.equals(apiName, conf.apiName) &&
                Objects.equals(physicalName, conf.physicalName) &&
                Objects.equals(longName, conf.longName) &&
                Objects.equals(category, conf.category) &&
                Objects.equals(description, conf.description) &&
                Objects.equals(dimensionFieldNames, conf.dimensionFieldNames) &&
                Objects.equals(defaultDimensionFieldNames, conf.defaultDimensionFieldNames) &&
                Objects.equals(allDimensionFields, conf.allDimensionFields) &&
                Objects.equals(defaultDimensionFields, conf.defaultDimensionFields) &&
                Objects.equals(keyValueStore, conf.keyValueStore) &&
                Objects.equals(searchProvider, conf.searchProvider) &&
                Objects.equals(aggregatable, conf.aggregatable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                dimensionName,
                apiName,
                physicalName,
                longName,
                category,
                description,
                dimensionFieldNames,
                defaultDimensionFieldNames,
                allDimensionFields,
                defaultDimensionFields,
                keyValueStore,
                searchProvider,
                aggregatable
        );
    }
}
