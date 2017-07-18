// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.util;

import com.yahoo.fili.webservice.config.FiliFeatureFlag;

import java.util.Locale;

/**
 * Provider for magic strings used in dimension keystore key generation.
 */
public class DimensionStoreKeyUtils {

    public static String getLastUpdatedKey() {
        return "last_updated_key";
    }

    /**
     * Returns a key for accessing all the dimension values in a
     * {@link com.yahoo.fili.webservice.data.dimension.KeyValueStore}.
     * <p>
     * When this key is passed into a {@link com.yahoo.fili.webservice.data.dimension.KeyValueStore}, the
     * KeyValueStore will return a String representation of a list of all the dimension values stored in the key value
     * dimension store.
     *
     * @return A key for accessing a list of all the values in a KeyValueStore as a String.
     */
    public static String getAllValuesKey() {
        return "all_values_key";
    }

    /**
     * Returns a key that allows access to the dimension rows of a given dimension.
     * <p>
     * Given a dimension field name (such as "id"), and the id of the dimension desired, returns a key. When
     * this key is passed into the appropriate {@link com.yahoo.fili.webservice.data.dimension.KeyValueStore},
     * the KeyValueStore will return the metadata of the associated dimension.
     *
     * @param rowName  The dimension field name to be appended to the beginning of the key
     * @param rowValue  The id of the dimension whose data is desired
     *
     * @return A key that, when passed into the appropriate KeyValueStore, will return the associated dimension
     * metadata.
     */
    public static String getRowKey(String rowName, String rowValue) {
        String key = "";
        if (rowName != null) {
            key = FiliFeatureFlag.CASE_SENSITIVE_KEYS.isOn() ? rowName : rowName.toLowerCase(Locale.ENGLISH);
        }
        if (rowValue != null) {
            key += "_" + (
                    FiliFeatureFlag.CASE_SENSITIVE_KEYS.isOn() ? rowValue : rowValue.toLowerCase(Locale.ENGLISH)
            );
        }
        return key + "_row_key";
    }

    /**
     *  Returns a key that, when fed into a {@link com.yahoo.fili.webservice.data.dimension.KeyValueStore} returns the
     *  field name used by Lucene.
     *
     * @param columnName  The name of the column to extract from Lucene.
     *
     * @return A key that, when fed into a KeyValueStore, returns the ID used to query a dimension in Lucene.
     */
    public static String getColumnKey(String columnName) {
        String key = "";
        if (columnName != null) {
            key = FiliFeatureFlag.CASE_SENSITIVE_KEYS.isOn()
                    ? columnName : columnName.toLowerCase(Locale.ENGLISH);
        }
        return key + "_column_key";
    }

    /**
     * Returns a key that, when fed into a {@link com.yahoo.fili.webservice.data.dimension.KeyValueStore} returns the
     * cardinality of the dimensions.
     *
     * @return  A key that, when fed into a KeyValueStore returns the cardinality of the dimensions.
     */
    public static String getCardinalityKey() {
        return "cardinality_key";
    }
}
