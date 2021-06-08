// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import static com.yahoo.bard.webservice.data.config.dimension.NamelessDimensionField.EMPTY;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.VirtualDimension;
import com.yahoo.bard.webservice.data.dimension.metadata.StorageStrategy;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class SimpleVirtualDimension implements Dimension, VirtualDimension {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleVirtualDimension.class);

    public static final String ERROR_MESSAGE = "Virtual dimensions do not support storage operations.";
    public static final String ROW_PARSE_ERROR = "SimpleVirtualDimension rows does not have fields: %s";

    public static final Set<DimensionField> FIELDS = Collections.singleton(EMPTY);
    public static final String KEY_FIELD_NAME = "";

    String apiName;

    /**
     * Constructor.
     *
     * @param apiName the apiName for the virtual dimension
     */
    public SimpleVirtualDimension(String apiName) {
        this.apiName = apiName;
    }


    protected static DimensionRow buildSimpleRow(String value) {
        return new DimensionRow(EMPTY, Collections.singletonMap(EMPTY, value));
    }

    @Override
    public void setLastUpdated(final DateTime lastUpdated) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public String getApiName() {
        return apiName;
    }

    @Override
    public String getDescription() {
        return apiName;
    }

    @Override
    public DateTime getLastUpdated() {
        return null;
    }

    @Override
    public LinkedHashSet<DimensionField> getDimensionFields() {
        return new LinkedHashSet<>(FIELDS);
    }

    @Override
    public LinkedHashSet<DimensionField> getDefaultDimensionFields() {
        return new LinkedHashSet<>(FIELDS);
    }

    @Override
    public DimensionField getFieldByName(final String name) {
        return "".equals(name) ? EMPTY : null;
    }

    @Override
    public SearchProvider getSearchProvider() {
        return null;
    }

    @Override
    public void addDimensionRow(final DimensionRow dimensionRow) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public void addAllDimensionRows(final Set<DimensionRow> dimensionRows) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public DimensionRow findDimensionRowByKeyValue(String value) {
        return buildSimpleRow(value);
    }

    @Override
    public DimensionField getKey() {
        return EMPTY;
    }

    @Override
    public DimensionRow parseDimensionRow(final Map<String, String> fieldNameValueMap) {
        if (! Collections.singleton(KEY_FIELD_NAME).equals(fieldNameValueMap.keySet())) {
            String error = String.format(ROW_PARSE_ERROR, fieldNameValueMap.keySet().toString());
            LOG.info(error);
            throw new IllegalArgumentException(error);
        }
        return buildSimpleRow(fieldNameValueMap.get(""));
    }

    @Override
    public DimensionRow createEmptyDimensionRow(final String keyFieldValue) {
        return buildSimpleRow(keyFieldValue);
    }

    @Override
    public String getCategory() {
        return "";
    }

    @Override
    public String getLongName() {
        return apiName;
    }

    @Override
    public StorageStrategy getStorageStrategy() {
        return null;
    }

    @Override
    public int getCardinality() {
        return 0;
    }

    @Override
    public boolean isAggregatable() {
        return true;
    }
}
