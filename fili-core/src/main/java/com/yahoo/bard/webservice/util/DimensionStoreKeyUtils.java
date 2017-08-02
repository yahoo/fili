// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.config.BardFeatureFlag;

import java.util.Locale;

import javax.validation.constraints.NotNull;

/**
 * Provider for magic strings used in dimension keystore key generation.
 */
public class DimensionStoreKeyUtils {

    public static final String ROW_KEY_SUFFIX = "_row_key";
    public static final String KEY_SEPARATOR = "_";

    public static String getLastUpdatedKey() {
        return "last_updated_key";
    }

    /**
     * Returns a key for accessing all the dimension values in a
     * {@link com.yahoo.bard.webservice.data.dimension.KeyValueStore}.
     * <p>
     * When this key is passed into a {@link com.yahoo.bard.webservice.data.dimension.KeyValueStore}, the
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
     * this key is passed into the appropriate {@link com.yahoo.bard.webservice.data.dimension.KeyValueStore},
     * the KeyValueStore will return the metadata of the associated dimension.
     *
     * @param fieldName  The dimension field name to be appended to the beginning of the key
     * @param fieldValue  The key of the dimension whose data is desired
     *
     * @return A key that, when passed into the appropriate KeyValueStore, will return the associated dimension value.
     */
    public static String getRowKey(@NotNull String fieldName, String fieldValue) {
        boolean caseSensitive = BardFeatureFlag.CASE_SENSITIVE_KEYS.isOn();
        String lookupFieldValue = fieldValue == null ? "" : fieldValue;

        return new StringBuilder()
                .append(caseSensitive ? fieldName : fieldName.toLowerCase(Locale.ENGLISH))
                .append(KEY_SEPARATOR)
                .append(caseSensitive ? lookupFieldValue : lookupFieldValue.toLowerCase(Locale.ENGLISH))
                .append(ROW_KEY_SUFFIX)
                .toString();
    }

    /**
     *  Returns a key that, when fed into a {@link com.yahoo.bard.webservice.data.dimension.KeyValueStore} returns the
     *  field name used by Lucene.
     *
     * @param columnName  The name of the column to extract from Lucene.
     *
     * @return A key that, when fed into a KeyValueStore, returns the ID used to query a dimension in Lucene.
     */
    public static String getColumnKey(String columnName) {
        String key = "";
        if (columnName != null) {
            key = BardFeatureFlag.CASE_SENSITIVE_KEYS.isOn()
                    ? columnName : columnName.toLowerCase(Locale.ENGLISH);
        }
        return key + "_column_key";
    }

    /**
     * Returns a key that, when fed into a {@link com.yahoo.bard.webservice.data.dimension.KeyValueStore} returns the
     * cardinality of the dimensions.
     *
     * @return  A key that, when fed into a KeyValueStore returns the cardinality of the dimensions.
     */
    public static String getCardinalityKey() {
        return "cardinality_key";
    }
}
