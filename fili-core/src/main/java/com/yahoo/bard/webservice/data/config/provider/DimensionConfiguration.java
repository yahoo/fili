package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;

import java.util.Set;

/**
 * Everything (...almost) you need to know about a dimension.
 */
public class DimensionConfiguration {

    protected final String apiName;
    protected final String longName;
    protected final String category;
    protected final String physicalName;
    protected final String description;
    protected final Set<String> fields;
    protected final Set<String> defaultFields;
    protected final boolean aggregatable;
    protected final KeyValueStore keyValueStore;
    protected final SearchProvider searchProvider;

    /**
     * Construct a new dimension configuration object.
     *
     * @param apiName  The API name
     * @param longName  The long name
     * @param category  The category
     * @param physicalName  The physical name
     * @param description  The description
     * @param fields  The dimension fields
     * @param defaultFields  The default dimension fields
     * @param aggregatable  Whether this dimension can be aggregated
     * @param keyValueStore  The key value store
     * @param searchProvider  The search provider
     */
    public DimensionConfiguration(String apiName, String longName, String category, String physicalName,
            String description, Set<String> fields, Set<String> defaultFields, boolean aggregatable,
            KeyValueStore keyValueStore, SearchProvider searchProvider
            ) {
        this.apiName = apiName;
        this.longName = longName;
        this.category = category;
        this.physicalName = physicalName;
        this.description = description;
        this.fields = fields;
        this.defaultFields = defaultFields;
        this.aggregatable = aggregatable;
        this.keyValueStore = keyValueStore;
        this.searchProvider = searchProvider;
    }


    /**
     * Get the API name.
     *
     * @return The API name
     */
    public String getApiName() {
        return apiName;
    }

    /**
     * Get the long name.
     *
     * @return The long name
     */
    public String getLongName() {
        return longName;
    }

    /**
     * Get the category.
     *
     * @return The category
     */
    public String getCategory() {
        return category;
    }

    /**
     * Get the physical name.
     *
     * @return The physical name
     */
    public String getPhysicalName() {
        return physicalName;
    }

    /**
     * Get the description.
     *
     * @return The description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the dimension fields.
     *
     * @return The fields
     */
    public Set<String> getFields() {
        return fields;
    }

    /**
     * Get the default dimension fields
     *
     * @return The default dimension fields
     */
    public Set<String> getDefaultFields() {
        return defaultFields;
    }

    /**
     * Get the key value store.
     *
     * @return The key value store
     */
    public KeyValueStore getKeyValueStore() {
        return keyValueStore;
    }

    /**
     * Get the search provider.
     *
     * @return The search provider
     */
    public SearchProvider getSearchProvider() {
        return searchProvider;
    }

    /**
     * Return true if the dimension is aggregatable.
     *
     * @return True if this dimension is aggregatable
     */
    public boolean isAggregatable() {
        return aggregatable;
    }
}
