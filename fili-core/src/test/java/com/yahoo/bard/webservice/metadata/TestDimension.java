// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;
import com.yahoo.bard.webservice.util.UnmodifiableLinkedHashSet;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Mutable dimension implementation designed to be easily used for testing.
 */
public class TestDimension implements Dimension {

    private static final String MISSING_ROW_KEY_FORMAT = "Dimension row '%s' doesn't contain expected key '%s'";

    private String apiName;
    private String description;
    private String category;
    private String longName;
    private DateTime lastUpdated;
    private DimensionField keyField;
    private LinkedHashSet<DimensionField> dimensionFields;
    private LinkedHashSet<DimensionField> defaultDimensionFields;
    private StorageStrategy storageStrategy = StorageStrategy.NONE;

    private SearchProvider searchProvider;
    private Map<String, DimensionRow> rows;

    /**
     * Constructor. Only apiName is set through the constructor. Use the state mutation methods to set the state of
     * this dimension.
     *
     * @param apiName  The api name of this dimension.
     */
    public TestDimension(String apiName) {
        this.apiName = apiName;
        this.dimensionFields = new LinkedHashSet<>();
        this.defaultDimensionFields = new LinkedHashSet<>();
        this.rows = new HashMap<>();
    }

    // STATE CONTROL
    // the following methods are used to control state of the test implementation. Only your test will interact with
    // these methods.

    /**
     * Setter.
     *
     * @param apiName  api name of the dimension
     */
    public void setApiName(String apiName) {
        this.apiName = apiName;
    }

    /**
     * Setter.
     *
     * @param description  description of the dimension
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Setter.
     *
     * @param category  category of the dimension
     */
    public void setCategory(String category) {
        this.category = category;
    }

    /**
     * Setter.
     * @param longName  long name (UI name) of the dimension
     */
    public void setLongName(String longName) {
        this.longName = longName;
    }

    /**
     * Setter.
     *
     * @param storageStrategy  storage strategy of the dimension
     */
    public void setStorageStrategy(StorageStrategy storageStrategy) {
        this.storageStrategy = storageStrategy;
    }

    /**
     * Adds the provided dimension field to the set of dimension fields. Additionally, this field is set as the key
     * field.
     *
     * @param kf  The key dimension field
     * @return if this dimension field had already been added to the dimension
     */
    public boolean addKeyField(DimensionField kf) {
        boolean present = addDimensionField(kf);
        setKeyField(kf);
        return present;
    }

    /**
     * Adds a {@link DimensionField} to the dimension.
     *
     * @param f  The field to add.
     * @return if the set already contained that dimension field
     */
    public boolean addDimensionField(DimensionField f) {
        return dimensionFields.add(f);
    }

    /**
     * Removes the provided {@link DimensionField}.
     *
     * @param f  The dimension field to remove
     * @return if the field was removed successfully.
     */
    public boolean removeDimensionField(DimensionField f) {
        return dimensionFields.remove(f);
    }

    /**
     * Resets the dimension fields to an empty set.
     */
    public void clearDimensionFields() {
        dimensionFields = new LinkedHashSet<>();
    }

    /**
     * Sets which dimension field is the key field of the dimension.
     *
     * @param keyField  The {@link DimensionField} to be set as the key field.
     */
    public void setKeyField(DimensionField keyField) {
        this.keyField = keyField;
    }

    /**
     * Sets the dimension fields of this dimension to the provided fields. An immutable copy of the provided fields is
     * made.
     *
     * @param newFields  The fields for this dimension
     */
    public void setDimensionFields(LinkedHashSet<DimensionField> newFields) {
        this.dimensionFields = new LinkedHashSet<>(newFields);
    }

    /**
     * Sets the default dimension fields of this dimension. The default fields should likely be a subset of the fields
     * on this dimension, but that is not enforced.
     *
     * @param defaultDimensionFields  The set of default fields.
     */
    public void setDefaultDimensionFields(LinkedHashSet<DimensionField> defaultDimensionFields) {
        this.defaultDimensionFields = new LinkedHashSet<>(defaultDimensionFields);
    }

    /**
     * Copies all of the current dimension fields on this dimension to be the default dimension fields.
     */
    public void setAllDimensionFieldsAsDefaultFields() {
        this.defaultDimensionFields = new LinkedHashSet<>(defaultDimensionFields);
    }

    /**
     * Clears the default dimension fields on this dimension.
     */
    public void clearDefaultDimensionFields() {
        this.defaultDimensionFields = new LinkedHashSet<>();
    }

    /**
     * Sets {@link SearchProvider} for this dimension.
     * TODO: write a search provider implementation that works out of the box with this dimension. Should be an inner
     * class
     *
     * @param searchProvider  The search provider for this dimension.
     */
    public void setSearchProvider(SearchProvider searchProvider) {
        this.searchProvider = searchProvider;
    }

    // DIMENSION METHODS
    // methods on the dimension interface. Fili classes will interact with these methods. Some of these mutate state.
    @Override
    public void setLastUpdated(DateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Override
    public String getApiName() {
        return apiName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public DateTime getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public LinkedHashSet<DimensionField> getDimensionFields() {
        return UnmodifiableLinkedHashSet.of(new LinkedHashSet<>(dimensionFields));
    }

    @Override
    public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
        return UnmodifiableLinkedHashSet.of(new LinkedHashSet<>(defaultDimensionFields));
    }

    @Override
    public DimensionField getFieldByName(String name) {
        return dimensionFields.stream()
                .filter(d -> d.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "requested field " + name +
                                " on dimension " + getApiName() +
                                " but that field is not supported on that dimension"

                ));
    }

    @Override
    public SearchProvider getSearchProvider() {
        return searchProvider;
    }

    @Override
    public void addDimensionRow(DimensionRow dimensionRow) {
        rows.put(dimensionRow.getKeyValue(), dimensionRow);
    }

    @Override
    public void addAllDimensionRows(Set<DimensionRow> dimensionRows) {
        dimensionRows.forEach(this::addDimensionRow);
    }

    @Override
    public DimensionRow findDimensionRowByKeyValue(String value) {
        return rows.get(value);
    }

    @Override
    public DimensionField getKey() {
        return keyField;
    }

    @Override
    public DimensionRow parseDimensionRow(Map<String, String> fieldNameValueMap) {
        String desc = fieldNameValueMap.remove("description");
        if (desc != null) {
            fieldNameValueMap.put("desc", desc);
        }

        LinkedHashMap<DimensionField, String> dimensionRowFieldValues = new LinkedHashMap<>(fieldNameValueMap.size());
        // Load every field we expect and only fields we expect
        for (DimensionField field : dimensionFields) {
            String fieldName = field.getName();
            String value = fieldNameValueMap.get(fieldName);
            if (value == null) {
                // A missing key value is unacceptable
                if (field == getKey()) {
                    String error = String.format(MISSING_ROW_KEY_FORMAT, fieldNameValueMap.toString(), fieldName);
                    throw new IllegalArgumentException(error);
                }
                // A missing value for another field is turned into the empty string
                value = "";
            }
            dimensionRowFieldValues.put(field, value);
        }
        return new DimensionRow(getKey(), dimensionRowFieldValues);
    }

    @Override
    public DimensionRow createEmptyDimensionRow(String keyFieldValue) {
        Map<DimensionField, String> rowMap = new HashMap<>();
        rowMap.put(getKey(), keyFieldValue);
        return new DimensionRow(getKey(), rowMap);
    }

    @Override
    public String getCategory() {
        return category == null ? getApiName() : category;
    }

    @Override
    public String getLongName() {
        return longName == null ? getApiName() : longName;
    }

    @Override
    public StorageStrategy getStorageStrategy() {
        return storageStrategy;
    }

    @Override
    public int getCardinality() {
        return rows.size();
    }

    @Override
    public boolean isAggregatable() {
        return false;
    }
}
