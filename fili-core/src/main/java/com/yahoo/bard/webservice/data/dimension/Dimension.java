// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.druid.serializers.DimensionToDefaultDimensionSpec;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.joda.time.DateTime;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Dimension interface.
 * <p>
 * ApiName must be unique to corresponding domain.
 * <p>
 * NOTE: To override the default serialization, use the @JsonSerialize on the implementing class.
 *       Using @JsonSerialize with no parameters will provide default Jackson behavior (so things such
 *       as @JsonValue will work properly) or else you can provide your own custom serializers using the
 *       same approach.
 */
@JsonSerialize(using = DimensionToDefaultDimensionSpec.class)
public interface Dimension {

    String DEFAULT_CATEGORY = "General";

    /**
     * Setter for lastUpdated.
     *
     * @param lastUpdated  The date and time at which this Dimension was last updated
     */
    void setLastUpdated(DateTime lastUpdated);

    /**
     * Getter for api name.
     *
     * @return apiName
     */
    String getApiName();

    /**
     * Getter for description.
     *
     * @return description
     */
    String getDescription();

    /**
     * Getter for lastUpdated.
     *
     * @return lastUpdated
     */
    DateTime getLastUpdated();

    /**
     * Returns all dimension fields of this dimension.
     *
     * @return all dimension fields of this dimension
     */
    LinkedHashSet<DimensionField> getDimensionFields();

    /**
     * Returns the default dimension fields to be shown in the response.
     *
     * @return the default dimension fields to be shown in the response
     */
    LinkedHashSet<DimensionField> getDefaultDimensionFields();

    /**
     * Find dimension field by name.
     *
     * @param name  field name
     *
     * @return DimensionField
     *
     * @throws IllegalArgumentException if this dimension does not have a field with the specified name
     */
    DimensionField getFieldByName(String name);

    /**
     * Getter for search provider.
     *
     * @return search provider
     */
    SearchProvider getSearchProvider();

    /**
     * Add a dimension row to the dimension's set of rows.
     *
     * @param dimensionRow  DimensionRow to add
     */
    void addDimensionRow(DimensionRow dimensionRow);

    /**
     * Add all dimension rows to the dimension's set of rows.
     *
     * @param dimensionRows  Set of DimensionRows to add
     */
    void addAllDimensionRows(Set<DimensionRow> dimensionRows);

    /**
     * Get a dimension row given an id.
     *
     * @param value  key value
     *
     * @return a dimension row - returns the first one found if there are multiple, or null if no matching row is found
     */
    DimensionRow findDimensionRowByKeyValue(String value);

    /**
     * Get primary key field for this dimension.
     *
     * @return primary key field
     */
    DimensionField getKey();

    /**
     * Generate a DimensionRow for this dimension from a field name / value map.
     *
     * @param fieldNameValueMap  Map of field names to values
     *
     * @return A DimensionRow with the schema of this Dimension
     */
    DimensionRow parseDimensionRow(Map<String, String> fieldNameValueMap);

    /**
     * Create an empty DimensionRow for this dimension.
     *
     * @param keyFieldValue  String value of the key field
     *
     * @return empty dimensionRow
     */
    DimensionRow createEmptyDimensionRow(String keyFieldValue);

    /**
     * Get the category of the dimension.
     *
     * @return category
     */
    String getCategory();

    /**
     * Get the long name of the dimension.
     *
     * @return long name
     */
    String getLongName();

    /**
     * Returns the {@link com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy} of the dimension.
     *
     * @return the storage strategy of the dimension.
     */
    StorageStrategy getStorageStrategy();

    /**
     * Get the cardinality of the dimension.
     *
     * @return cardinality as an int
     */
    int getCardinality();

    /**
     * Return whether this dimension can be aggregated.
     *
     * @return  true if this dimension is aggregatable
     */
    boolean isAggregatable();
}
